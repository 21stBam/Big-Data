import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import org.apache.commons.lang.StringEscapeUtils;

public class UsersSummary{
    public static class SummaryTuple implements Writable {
        private Date min = new Date();
        private Date max = new Date();
        private long count = 0;
        private long postSum = 0;

        public Date getMin() {
            return min;
        }

        public void setMin(Date min) {
            this.min = min;
        }

        public Date getMax() {
            return max;
        }

        public void setMax(Date max) {
            this.max = max;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        public long getPostSum() {
            return postSum;
        }

        public void setPostSum(long postSum) {
            this.postSum = postSum;
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            min = new Date(in.readLong());
            max = new Date(in.readLong());
            count = in.readLong();
            postSum = in.readLong();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(min.getTime());
            out.writeLong(max.getTime());
            out.writeLong(count);
            out.writeLong(postSum);
        }
    }

    
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


    public static class SOUsersSummaryMapper extends Mapper<Object, Text, Text, SummaryTuple> {
        private Text user = new Text();
        private SummaryTuple summary = new SummaryTuple();
        private final static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
          
            // Parse the input string into a nice map
            Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

            // Extract variables for summary object
            String userId = parsed.get("UserId");
            String text = parsed.get("Text");
            String creationString = parsed.get("CreationDate");
              
            // Will skip record if the UserId is not set
            if (userId == null || text == null || creationString == null) {
                // skip this record
                return;
            }

            // Unescape the HTML because the SO data is escaped.
            text = StringEscapeUtils.unescapeHtml(text.toLowerCase());

            // Remove some annoying punctuation
            text = text.replaceAll("'", ""); // remove single quotes (e.g., can't)
            text = text.replaceAll("[^a-zA-Z]", " "); // replace the rest with a

            // Adding data to summary Object, then writing it to the context
            try {
                Date creationDate = format.parse(creationString);
                
                summary.setMax(creationDate);
                summary.setMin(creationDate);
                summary.setCount(1);
                summary.setPostSum(text.length());

                user.set(userId);

                context.write(user, summary);
            } catch (ParseException e) {
                // An error occurred parsing the creation Date string
                // skip this record
                System.out.println("Creation String " + creationString + " had an error message: "+ e.getMessage());
            }
        }
    }


    public static class SOUsersSummaryCombiner extends Reducer<Text, SummaryTuple, Text, SummaryTuple> {
        private SummaryTuple result = new SummaryTuple();

        @Override
        public void reduce(Text key, Iterable<SummaryTuple> values, Context context) throws IOException, InterruptedException {
            // Inialized our result
            result.setMin(null);
            result.setMax(null);
            int postSum = 0;
            int count = 0;

            // Loop through SummaryTuples to fill in result SummaryTuple object
            for (SummaryTuple value : values) {
                if (result.getMin() == null || value.getMin().compareTo(result.getMin()) < 0) {
                    result.setMin(value.getMin());
                }

                if (result.getMax() == null || value.getMax().compareTo(result.getMax()) > 0) {
                    result.setMax(value.getMax());
                }

                postSum += value.getPostSum();
                count += 1;
            }

            result.setCount(count);
            result.setPostSum(postSum);

            context.write(key, result);
        }
    }
    
    
    public static class SOUsersSummaryReducer extends Reducer<Text, SummaryTuple, Text, Text> {
        private Text result = new Text();
        private String template = " has posted %d times, first post : %s, last post :  %s, average length : %d";

        @Override
        public void reduce(Text key, Iterable<SummaryTuple> values, Context context) throws IOException, InterruptedException {
            Date min = null;
            Date max = null;
            int count = 0;
            int postSum = 0;

            // Looping through SummaryTuples to get min, max, count, and postSum
            for (SummaryTuple value : values) {
                if (min == null || min.compareTo(value.getMin()) < 0) {
                    min = value.getMin();
                }

                if (max == null || max.compareTo(value.getMax()) > 0) {
                    max = value.getMax();
                }

                count += value.getCount();
                postSum +=  value.getPostSum();
            }

            // Finding the average, then Setting the results and filling out the template String
            int avg = postSum / count; 
            result.set(String.format(template, count, min.toString(), max.toString(), avg, postSum));
            
            context.write(new Text("User " + key.toString()), result);
        }
    }
   

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
          System.err.println("Usage: UsersSummary <in> <out>");
          System.exit(2);
        }

        Job job = new Job(conf, "StackOverflow Users Summary");
        job.setJarByClass(UsersSummary.class);
        job.setMapperClass(SOUsersSummaryMapper.class);
        job.setCombinerClass(SOUsersSummaryCombiner.class);
        job.setReducerClass(SOUsersSummaryReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SummaryTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
   }
}
