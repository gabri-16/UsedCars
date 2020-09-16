import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.Car;

public class Opi {

    public static class OpiMapper extends Mapper<Object, Text, Text, DoubleWritable>{
        
        private int price;
        private Text brand = new Text();
        private int odometer;

        private DoubleWritable opi = new DoubleWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");

            price = Integer.parseInt(tokens[1].trim());
            brand.set(tokens[2].trim());
            odometer = Integer.parseInt(tokens[4].trim()); 
          
            opi.set(((double) odometer) / price);
            context.write(brand, opi);            
        }
    }

    public static class OpiReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        private DoubleWritable avgOpi = new DoubleWritable(); 

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (final DoubleWritable opi: values) {
               sum += opi.get();
               count++;
            }          
            avgOpi.set(sum / count);
            context.write(key, avgOpi);
	}
    }

    public static void main(String[] args) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

	if(args.length > 2 && Integer.parseInt(args[2]) >= 0) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
	}

        job.setJarByClass(Opi.class);

        job.setMapperClass(OpiMapper.class);
	job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);	

	job.setReducerClass(OpiReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);

	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
