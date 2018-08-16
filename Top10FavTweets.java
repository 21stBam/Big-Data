import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//
// Written by Obsa Aba-waji
//
public class Top10FavTweets  {
    public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
        private TreeMap<Integer, Text> tweetsToRecordMap = new TreeMap<Integer, Text>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",", -1);
            String handle = line[0];
            String tweet = "";
            int i = 1;
            if (line.length >= 6) {
                do {
                    tweet += line[i];
                    i++;
                } 
                while (i + 5 <= line.length); 
            }
            String favs = line[++i];

            if (handle.equals("") || tweet.equals("") || favs.equals("")) {
              // skip this record
              return;
            } else if( handle.equals("handle") || tweet.equals("Tweet") || favs.equals("Favs")) {
              // skip this record likely first line
              return;
            }
            

            tweetsToRecordMap.put(Integer.parseInt(favs), new Text(value));

            // Treemap is sorted from smallest to largest, 
            // so I remove any record above 10 from the top
            if (tweetsToRecordMap.size() > 10) {
                tweetsToRecordMap.remove(tweetsToRecordMap.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text txt: tweetsToRecordMap.values()) {
                context.write(NullWritable.get(), txt);
            }
        }
    }  

    public static class TopTenReducer extends Reducer<NullWritable, Text, Text, Text> {
        private TreeMap<Integer, Text> tweetsToRecordMap = new TreeMap<Integer, Text>();
        private String path = "";
        
        private Configuration conf;
        private BufferedReader fis;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            path = conf.get("toptenfavtweets.users");
        }

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            for (Text val : values) {
                String[] line = val.toString().split(",", -1);
                String handle = line[0];
                String tweet = "";
                int i = 1;
                if (line.length >= 6) {
                    do {
                        tweet += line[i];
                        i++;
                    } 
                    while (i + 5 <= line.length); 
                }
                String favs = line[++i];

                tweetsToRecordMap.put(Integer.parseInt(favs), new Text(val));

                // Treemap is sorted from smallest to largest, 
                // so I remove any record above 10 from the top
                if (tweetsToRecordMap.size() > 10) {
                  tweetsToRecordMap.remove(tweetsToRecordMap.firstKey());
                }
            }
            Path pt = new Path("hdfs:" + path);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String fileLines;
            while((fileLines = fis.readLine()) != null) {
                String[] fileLine = fileLines.split(",", -1);
                String fileHandle = fileLine[0];
                for (Text txt: tweetsToRecordMap.descendingMap().values()) {
                    if (txt.toString().contains(fileHandle)) {
                        // writing the key: user details  value : tweets
                        context.write(new Text(fileLines), txt);
                    }  
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
          System.err.println("Usage: Top10FavTweets <tweets> <users> <out>");
          System.exit(2);
        }

        Job job = new Job(conf, "Top Ten Fav Tweets");
        job.setJarByClass(Top10FavTweets.class);
        job.setMapperClass(TopTenMapper.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().set("toptenfavtweets.users", otherArgs[1]);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
