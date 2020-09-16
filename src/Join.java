import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import utils.Car;
import utils.JoinPair;

public class Join {

    private static final int FLAG_1 = 1;
    private static final int FLAG_2 = 2;

    public static class JoinMapper extends Mapper<Text, Text, Text, JoinPair> {    
   
        Text brand = new Text();
        JoinPair outValue = new JoinPair();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {         
      
            if (isNumeric(value.toString().trim())) { // (brand, opi) -> (brand, (FLAG_1, opi))
                brand = key;
                outValue = new JoinPair(FLAG_1, value.toString().trim());
            } else { // (region, brand) -> (brand, (FLAG_2, region))
                brand = new Text(value.toString().trim());
                outValue = new JoinPair(FLAG_2, key.toString());       
            }

            context.write(brand, outValue);
        }

        private boolean isNumeric(final String input) {
            try {
                double i = Double.parseDouble(input);
            } catch (NumberFormatException e) {
                return false;
            }
            return true;
        }
    }

    public static class JoinReducer extends Reducer<Text, JoinPair, Text, Text> {

        Text regionOut = new Text();
        Text valueOut = new Text();

	public void reduce(Text key, Iterable<JoinPair> values, Context context) throws IOException, InterruptedException {

          // Can't banally use 2 nested for each...need first to separate elements so that 2 different iterator are used...
          final List<String> opi = new ArrayList();
          final List<String> region = new ArrayList();
                 
          for (final JoinPair p: values) {
            if (p.getFlag() == FLAG_1) {
              opi.add(p.getValue());
            } else {
              region.add(p.getValue());
            }
          }

          for (final String o: opi) {
            for (final String r: region) {
              regionOut.set(r);
              valueOut.set(key + " " + o);
              context.write(regionOut, valueOut);
            } 
          }
	}
    }

    public static void main(String[] args) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        Path inputPath1 = new Path(args[0]);
        Path inputPath2 = new Path(args[1]);
        Path outputPath = new Path(args[2]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

	if (args.length > 3 && Integer.parseInt(args[3]) >= 0) {
            job.setNumReduceTasks(Integer.parseInt(args[3]));
	}

        job.setJarByClass(Join.class);

        job.setMapperClass(JoinMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(JoinPair.class);		

	job.setReducerClass(JoinReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, inputPath1);
        FileInputFormat.addInputPath(job, inputPath2);
	FileOutputFormat.setOutputPath(job, outputPath);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
