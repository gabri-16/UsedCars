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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.Car;

public class Region {

    public static class RegionMapper extends Mapper<Object, Text, Text, Text> {
    
        private static final String GAS_FUEL = "gas";        

        private Text region = new Text(); 
        private Text brand = new Text();     
   
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(" ");            

            if (tokens[3].trim().equals(GAS_FUEL)) { 
                region.set(tokens[0].trim());
                brand.set(tokens[2].trim());
                context.write(region, brand);        
            }    
        }
    }

    public static class RegionReducer extends Reducer<Text, Text, Text, Text> {

        private Text topBrand = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            final Map<String, Integer> brandFrequency = new HashMap();
            for (final Text brand: values) {
               final String brandName = brand.toString();
               if (brandFrequency.containsKey(brandName)) {
                   brandFrequency.put(brandName, brandFrequency.get(brandName) + 1);
               } else {
                   brandFrequency.put(brandName, 1);
               }
            }
            
            Entry<String, Integer> max = null;           
            max = brandFrequency.entrySet().iterator().next();

            for (final Entry<String, Integer> e: brandFrequency.entrySet()) {
                if (max == null || e.getValue() > max.getValue()) {
                    max = e; 
                }
            }

            topBrand.set(max.getKey());          
            context.write(key, topBrand);
	}
    }

    public static void main(String[] args) throws Exception {
    
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "2b-region");

        Path inputPath = new Path(args[0]), outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(new Configuration());

        if (fs.exists(outputPath)) {
             fs.delete(outputPath, true);
        }

	if (args.length > 2 && Integer.parseInt(args[2]) >= 0) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
	}

        job.setJarByClass(Region.class);

        job.setMapperClass(RegionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);		

	job.setReducerClass(RegionReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(job, inputPath);
	FileOutputFormat.setOutputPath(job, outputPath);

	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
