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
import utils.OpiAveragePair;

public class OpiCombiner {

    public static class OpiCombinerMapper extends Mapper<Object, Text, Text, OpiAveragePair>{
        
        private int price;
        private Text brand = new Text();
        private int odometer;
        private double opi;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(" ");

            price = Integer.parseInt(tokens[1].trim());
            brand.set(tokens[2].trim());
            odometer = Integer.parseInt(tokens[4].trim()); 
            opi = ((double) odometer) / price;
            context.write(brand, new OpiAveragePair(opi, 1));            
        }
    }

    public static class OpiCombinerCombiner extends Reducer<Text, OpiAveragePair, Text, OpiAveragePair> {

      double avgOpi;

      public void reduce(Text key, Iterable<OpiAveragePair> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (final OpiAveragePair pair: values) {
               sum += pair.getAverage();
               count++;
            }          
            avgOpi = sum / count;
            context.write(key, new OpiAveragePair(avgOpi, count));        
      }
    }

    public static class OpiCombinerReducer extends Reducer<Text, OpiAveragePair, Text, DoubleWritable> {

        private DoubleWritable avgOpi = new DoubleWritable(); 

	public void reduce(Text key, Iterable<OpiAveragePair> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (final OpiAveragePair opi: values) {
               sum += opi.getAverage() * opi.getQuantity();
               count += opi.getQuantity();
            }          
            avgOpi.set(sum / count);
            context.write(key, avgOpi);
	}
    }

    public static void main(String[] args) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2a-opi");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

	if(args.length > 2 && Integer.parseInt(args[2]) >= 0) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
	}

        job.setJarByClass(OpiCombiner.class);

        job.setMapperClass(OpiCombinerMapper.class);
	job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OpiAveragePair.class);	

        job.setCombinerClass(OpiCombinerCombiner.class);

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);

	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
