import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.GenericOptionsParser;
//
// Written by Obsa Aba-waji
//
public class AboveAndBelowTenTweets {
    private static final String BELOW_TEN_TWEETS = "belowtentweets";
    private static final String ABOVE_TEN_TWEETS = "abovetentweets";

    public static class UserSummary implements Writable {
        private String details = "";
        private int tweetCount = 0;

        public void incrementTweetCounter() {
            this.tweetCount += 1;
        }

        public String getDetails() {
            return this.details;
        }
        public int getTweetCount() {
            return this.tweetCount;
        }
        public void setDetails(String details) {
            this.details = details;
        }
        public void setTweetCount(int tweetCount) {
            this.tweetCount = tweetCount;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            tweetCount = in.readInt();
            details = in.readLine();
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(tweetCount);
            out.writeChars(details);
        }
        @Override
        public String toString() {
            // TODO: Split and clean up
            return "Details " + details + " Count " + tweetCount;
        }
    }

    public static class TweetCounterMapper extends Mapper<Object, Text, Text, UserSummary> {
        private UserSummary user = new UserSummary();
        private Text newKey = new Text();

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
                } while( i + 5 <= line.length);
            }

            if (handle.equals("") || tweet.equals("")) {
                // skip this record
                return;
            }

            if (handle.equals("Handle")) {
                // skip this record
                return;
            }
            
            user.setTweetCount(1);
            user.setDetails("");

            newKey.set(handle);

            context.write(newKey, user);
        }
    }

    public static class AddUserDataMapper extends Mapper<Object, Text, Text, UserSummary> {
        private UserSummary user = new UserSummary();
        private Text newKey = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",", -1);
            String handle = line[0];

            if (handle.equals("") || handle.equals("Handle")) {
                // skip this record
                return;
            }

            user.setDetails(value.toString());
            user.setTweetCount(0);

            newKey.set(handle);

            context.write(newKey, user);
        }
    }

    public static class OutputReducer extends Reducer<Text, UserSummary, Text, UserSummary> {
        private MultipleOutputs<Text, UserSummary> mos;
        private UserSummary result = new UserSummary();

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            mos = new MultipleOutputs(context);
        }

        @Override
        public void reduce(Text key, Iterable<UserSummary> values, Context context) throws IOException, InterruptedException {
            String userDetail = "";
            int tweetTotal = 0;
            for(UserSummary user : values) {
                if (user.getDetails() == null || user.getDetails().equals("")) {
                    tweetTotal += user.getTweetCount();
                } else if ( user.getTweetCount() == 0) {
                    // result.setDetails(user.getDetails());
                    userDetail += user.getDetails();
                }
            }

            result.setTweetCount(tweetTotal);
            result.setDetails(userDetail);

            if (result.getTweetCount() > 10) {
                mos.write(ABOVE_TEN_TWEETS, key, result);
            } else if (result.getTweetCount() < 10 ) {
                mos.write(BELOW_TEN_TWEETS, key, result);
            } else {
                // skip these
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage AboveAndBelowTenTweets <tweets> <users> <out> ");
            System.exit(2);
        }

        Job job = new Job(conf, " Sorting Users By Number of Tweets ");
        job.setJarByClass(AboveAndBelowTenTweets.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, TweetCounterMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, AddUserDataMapper.class);

        job.setReducerClass(OutputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserSummary.class);

        MultipleOutputs.addNamedOutput(job, ABOVE_TEN_TWEETS, TextOutputFormat.class, Text.class, UserSummary.class);
        MultipleOutputs.addNamedOutput(job, BELOW_TEN_TWEETS, TextOutputFormat.class, Text.class, UserSummary.class);

        MultipleOutputs.setCountersEnabled(job, true);

        TextOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
