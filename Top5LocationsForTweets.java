import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringEscapeUtils;
//
// Written by Obsa Aba-waji
//
public class Top5LocationsForTweets {
    private static final String BIN_OUTPUT = "bins";

    public static class BinningMapper extends Mapper<Object, Text, Text, Text> {
        private Text key = new Text();
        private Map<String, String> userToLocation = new HashMap<String, String>();
        private static Set<String> locationBins = new TreeSet<String>();
        private Text locationKey = new Text();
        private String path = "";
        private Configuration conf;
        private BufferedReader fis;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            conf = context.getConfiguration();
            path = conf.get("topfivelocation.users");
            Path pt = new Path("hdfs:" + path);
            FileSystem fs = FileSystem.get(conf);
            fis = new BufferedReader(new InputStreamReader(fs.open(pt)));

            String lines;
            while((lines = fis.readLine()) != null) {
                String[] user = lines.split(",", -1);
                String handle = user[0];
                String location = user[user.length - 4] + user[user.length -3];

                if (handle.equals("Handle") || handle == null || location == null) {
                    // skip this record
                    continue;
                }
                
                // Clean up the location
                location = StringEscapeUtils.unescapeJava(location);

                location = location.replaceAll("\"", "");
                location = location.replaceAll("[^a-zA-Z]", " ");
                location = location.trim();
                location = location.replaceAll(" ", "_");

                // Check if handle is in map, if not add to map,
                // then add location to the Set
                if (!userToLocation.containsKey(handle)) {
                    userToLocation.put(handle, location);
                }
                locationBins.add(location);
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",", -1);
            String handle = line[0];

            // Check if the handle is inside of the userToLocation map
            // Get the location if it is, then write to the context
            // with the location as the key and tweet line as value
            if(userToLocation.containsKey(handle)) {
                String location = userToLocation.get(handle);
                locationKey.set(location);
                context.write(locationKey, value);
            } else {
                // First Line is skipped    
            }
        }

    }

    public static class BinReducer extends Reducer<Text, Text, Text, Text> {
        private MultipleOutputs<Text, Text> mos;
        private TreeMap<Integer, String> locationTweetCounter = new TreeMap<Integer, String>();
        private Text location = new Text();
        private Text count = new Text();
        private int total;

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tweets = "";
            int counter = 0;
            // iterate over tweets and increment counter as well as append
            // them to tweets String. 
            for (Text value: values) {
                String val = value.toString().replaceAll(":", "");
                tweets = tweets + ":" + val;
                counter++;
                total++;
            } 
            // Clean up Tweets
            tweets = tweets.replaceAll("[^A-Za-z0-9, :]", "");
            tweets = tweets.replace("\"", "");
            tweets += "\n\n";

            // Write out to bins file
            mos.write("bins", key, tweets);

            // Insert counter and location in TreeMap
            locationTweetCounter.put(counter, key.toString());
            // remove items from the TreeMap if it grows above 5.
            if (locationTweetCounter.size() > 5) {
                locationTweetCounter.remove(locationTweetCounter.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Iterate over the TreeMap and write results to file called "Top5"
            for (Map.Entry<Integer, String> entry: locationTweetCounter.entrySet()) {
                location.set(entry.getValue().toString());
                count.set(Integer.toString(entry.getKey()));
                mos.write("Top5",location, count);
            }
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: Top5LocationsForTweets <tweets> <users> <out>");
            System.exit(2);
        }

        Job binJob = new Job(conf, "Binning Tweets by Location");
        binJob.setJarByClass(Top5LocationsForTweets.class);
        binJob.setMapperClass(BinningMapper.class);
        binJob.setReducerClass(BinReducer.class);
        binJob.setOutputFormatClass(LazyOutputFormat.class);
        binJob.setOutputValueClass(Text.class);
        binJob.setOutputKeyClass(Text.class);
        LazyOutputFormat.setOutputFormatClass(binJob, TextOutputFormat.class);

        TextInputFormat.addInputPath(binJob, new Path(otherArgs[0]));
        binJob.getConfiguration().set("topfivelocation.users", otherArgs[1]);
        TextOutputFormat.setOutputPath(binJob, new Path(otherArgs[2]));

        MultipleOutputs.addNamedOutput(binJob, "bins", TextOutputFormat.class, Text.class, Text.class);
        MultipleOutputs.addNamedOutput(binJob, "Top5", TextOutputFormat.class, Text.class, Text.class);

        MultipleOutputs.setCountersEnabled(binJob, true);

        System.exit(binJob.waitForCompletion(true) ? 0 : 1);
    }
}
