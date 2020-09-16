import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;
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

import utils.Car;
import utils.JoinPair;

public class Join {

    public static class JoinMapper extends Mapper<Object, Text, Text, JoinPair> {    
   
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class JoinReducer extends Reducer<Text, JoinPair, NullWritable, Text> {

        final NullWritable nw = NullWritable.get();

	public void reduce(Text key, Iterable<JoinPair> values, Context context) throws IOException, InterruptedException {

            context.write(nw, new Text("test"));
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
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, inputPath1);
        FileInputFormat.addInputPath(job, inputPath2);
	FileOutputFormat.setOutputPath(job, outputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
