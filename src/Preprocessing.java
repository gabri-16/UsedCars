import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.Car;

public class Preprocessing {

    public static class PreprocessingMapper extends Mapper<LongWritable, Text, LongWritable, Car>{
        
        private static final String WHITESPACE_REGEX = "\\s+";
        private static final String WHITESPACE_REPLACER = "_";

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            try {   
                final String region = removeWhitespaces(tokens[2].trim());
                final int price = Integer.parseInt(tokens[4].trim());
                final String brand = removeWhitespaces(tokens[6].trim());
                final String fuel = removeWhitespaces(tokens[10].trim());
                final int odometer = Integer.parseInt(tokens[11].trim());
                final Car car = new Car(region, price, brand, fuel, odometer);
                if (!car.containsMissingData()) {
                  context.write(key, car);
                }
            } catch (NumberFormatException e) {

            } catch (IndexOutOfBoundsException e2) {
            
            }
        }

       private String removeWhitespaces(String input) {
           return input.replaceAll(WHITESPACE_REGEX, WHITESPACE_REPLACER);
       }
    }

    public static class PreprocessingReducer extends Reducer<LongWritable, Car, NullWritable, Car> {

        final NullWritable nw  = NullWritable.get();

	public void reduce(LongWritable key, Iterable<Car> values, Context context) throws IOException, InterruptedException {
          context.write(nw, values.iterator().next());
	}
    }

    public static void main(String[] args) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "1-preprocessing");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

      	if(args.length > 2 && Integer.parseInt(args[2]) >= 0) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
	}

        job.setJarByClass(Preprocessing.class);
        
        job.setMapperClass(PreprocessingMapper.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Car.class);
		
	job.setReducerClass(PreprocessingReducer.class);
	job.setOutputKeyClass(NullWritable.class);
	job.setOutputValueClass(Car.class);

	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
