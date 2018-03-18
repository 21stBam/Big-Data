import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
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
import org.apache.commons.lang.StringEscapeUtils;
import java.util.HashMap;

//
// Modified by Obsa Aba-waji
//

public class CommentWordCount {
	public static class MRDPUtils {
        	public static final String[] REDIS_INSTANCES = { "p0", "p1", "p2", "p3", "p4", "p6" };

	        // This helper function parses the stackoverflow into a Map for us.
        	public static Map<String, String> transformXmlToMap(String xml) {
                	Map<String, String> map = new HashMap<String, String>();
	                try {
        	                String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
	                        for (int i = 0; i < tokens.length - 1; i += 2) {
        	                        String key = tokens[i].trim();
                	                String val = tokens[i + 1];
	                                map.put(key.substring(0, key.length() - 1), val);
        	                }
	                } catch (StringIndexOutOfBoundsException e) {
        	                System.err.println(xml);
	                }
	                return map;
	        }
	}

	public static class SOWordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Parse the input string into a nice map
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

			// Grab the "PostHistoryTypeId" field, since that is what we are counting over
			String postHistoryTypeId = parsed.get("PostHistoryTypeId");
            // Check to see if Field exist, if not skip
            if (postHistoryTypeId == null) {
              // skip this record
              return;
            }

            // Checking the value of postHistoryTypeId
			if (postHistoryTypeId.equals("1")) {
				// Update Count for Question
                word.set("Question");
		        context.write(word, one);
			} else if (postHistoryTypeId.equals("2")){
				// Update Count for Response
                word.set("Response");
                context.write(word, one);
			} else {
              // Not of type Question or Response
            }
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}  
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: CommentWordCount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "StackOverflow Comment Word Count");
		job.setJarByClass(CommentWordCount.class);
		job.setMapperClass(SOWordCountMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
