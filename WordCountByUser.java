import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class WordCountByUser {
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private IntWritable count = new IntWritable();
        private Text user = new Text();

        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException {
            conf = context.getConfiguration();
            user = new Text(conf.get("wordcountbyuser.user"));
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",", -1);
            String handle = line[0];
            String txt = "";
            int i = 1;
            if (line.length >= 6) {
                do {
                    txt += line[i];
                    i++;
                } while (i + 5 <= line.length);
            }

            // Remove some annoying punctuation
            txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
            txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a space
            handle = handle.replaceAll("\"", "");

            if (handle.equals(user.toString())) {
                StringTokenizer st = new StringTokenizer(txt);
                count.set(st.countTokens());
                context.write(user, count);
            } else {
                // Handle does not match user passed in
                return;
            }
        }
    } 

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }

            result.set(sum);
            context.write(key, result);
        } 
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage TweetCountByUser <in> <user> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Twitter User Tweet Count");
        job.setJarByClass(WordCountByUser.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.getConfiguration().set("wordcountbyuser.user", otherArgs[1]);
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
