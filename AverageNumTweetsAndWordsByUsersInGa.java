import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//
// Written by Obsa Aba-waji
//
public class AverageNumTweetsAndWordsByUsersInGa {
    public static class UserStatWritable implements Writable {
        private long wordTotal;
        private long tweetTotal;

        public long getWordTotal() {
            return wordTotal;
        }
        public long getTweetTotal() {
            return tweetTotal;
        }
        public void setWordTotal(long wordTotal) {
            this.wordTotal = wordTotal;
        }
        public void setTweetTotal(long tweetTotal) {
            this.tweetTotal = tweetTotal;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            wordTotal = in.readLong();
            tweetTotal = in.readLong();
        }
        @Override 
        public void write(DataOutput out) throws IOException {
            out.writeLong(wordTotal);
            out.writeLong(tweetTotal);
        }
    }


    public static class ANTWMapper extends Mapper<Object, Text, Text, UserStatWritable> {
        private static UserStatWritable stat = new UserStatWritable();
        private String path;
        
        private Configuration conf;
        private BufferedReader fis;

        List<String> handlesInGa = new ArrayList<String>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            path = conf.get("averagenumtweetsandwords.users");
            Path pt = new Path("hdfs:" + path);
            FileSystem fs = FileSystem.get(conf);
            fis = new BufferedReader(new InputStreamReader(fs.open(pt)));
            String lines;
            while ((lines = fis.readLine()) != null) {
                if (lines.contains("GA") || lines.contains("Georgia")) {
                    String[] line = lines.split(",", -1);
                    String handle = line[0];
                    handlesInGa.add(handle);
                }
            }
        }

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
              } while (i + 5 <= line.length);
            }

            for (String gahandle : handlesInGa) {
                if (gahandle.equals(handle)) {
                    // Create Record to be written
                    tweet = tweet.replaceAll("'", ""); // remove all single quotes
                    tweet = tweet.replaceAll("[^a-zA-z0-9]", " ");
                    StringTokenizer tokenizedTweet = new StringTokenizer(tweet);
                    stat.setWordTotal(tokenizedTweet.countTokens());
                    stat.setTweetTotal(1L);

                    context.write(new Text(handle), stat);
                }
            }
        }
    }

    public static class ANTWCombiner extends Reducer<Text, UserStatWritable, Text, UserStatWritable> {
        private static UserStatWritable result = new UserStatWritable();

        @Override
        public void reduce(Text key, Iterable<UserStatWritable> values, Context context) throws IOException, InterruptedException {
            long tweetTotal = 0;
            long wordTotal = 0;
            for(UserStatWritable stat : values) {
                tweetTotal += stat.getTweetTotal();
                wordTotal += stat.getWordTotal(); 
            }
            result.setTweetTotal(tweetTotal);
            result.setWordTotal(wordTotal);

            context.write(key, result);
        }
    }

    public static class ANTWReducer extends Reducer<Text, UserStatWritable, Text, Text> {
        private String template = "Users from Georgia have tweeted an average of %d tweets and %d words";

        private long tweetTotal = 0;
        private long wordTotal = 0;
        private long keyTotal = 0;
        @Override
        public void reduce(Text key, Iterable<UserStatWritable> values, Context context) throws IOException, InterruptedException {
            for(UserStatWritable stat: values) {
                tweetTotal += stat.getTweetTotal();
                wordTotal += stat.getWordTotal();
            }
            keyTotal += 1;
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text(""), new Text(String.format(template, tweetTotal / keyTotal, wordTotal / tweetTotal)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: AverageNumTweetsAndWordsByUsersInGa <tweets> <users> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Average Number of Tweets and Words by Users in GA");
        job.setJarByClass(AverageNumTweetsAndWordsByUsersInGa.class);
        job.setMapperClass(ANTWMapper.class);
        job.setReducerClass(ANTWReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputValueClass(UserStatWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().set("averagenumtweetsandwords.users", otherArgs[1]);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
