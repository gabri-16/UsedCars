import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.Car;
//15
public class Project {

    public static class ProjectMapper extends Mapper<Object, Text, Text, Car>{
        
        private static final String WHITESPACE_REGEX = "\\s+";
        private static final String WHITESPACE_REPLACER = "_";

        private String region;
        private int price;
        private String brand;
        private String fuel;
        private int odometer;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            try {   
                region = removeWhitespaces(tokens[2].trim());
                price = Integer.parseInt(tokens[4].trim());
                brand = removeWhitespaces(tokens[6].trim());
                fuel = removeWhitespaces(tokens[10].trim());
                odometer = Integer.parseInt(tokens[11].trim());

                Car car = new Car(region, price, brand, fuel, odometer);
                Text dummyKey = new Text(tokens[0].trim());
                context.write(dummyKey, car);

            } catch (NumberFormatException e) {

            }
        }

       private String removeWhitespaces(String input) {
           return input.replaceAll(WHITESPACE_REGEX, WHITESPACE_REPLACER);
       }
    }

    public static class ProjectReducer extends Reducer<Text, Car, Text, Car> {

	public void reduce(Text key, Iterable<Car> values, Context context) throws IOException, InterruptedException {
          context.write(key, values.iterator().next());
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

        job.setJarByClass(Project.class);
        job.setMapperClass(ProjectMapper.class);
		
	if(args.length>2){
	    if(Integer.parseInt(args[2])>=0){
	        job.setNumReduceTasks(Integer.parseInt(args[2]));
            }
	}

	job.setReducerClass(ProjectReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Car.class);

	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
