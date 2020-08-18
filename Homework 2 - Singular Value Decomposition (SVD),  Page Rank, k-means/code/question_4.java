import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Kmeans extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		// To see the arguments passed to the function
		System.out.println(Arrays.toString(args));
		
		int res = 0;
		for(int i=0; i<20; i++) {
			// Add an argument for iteration number
			args[2] = Integer.toString(i);
			res = ToolRunner.run(new Configuration(), new Kmeans(), args);
		}
		System.exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// To see the arguments passed to the function
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "Kmeans");

		// Extract the current iteration number from args
		int curr = Integer.parseInt(args[2]);
		int next = curr + 1;
		
		// Set the configurations: input, output and cost directory
		Configuration conf = job.getConfiguration();
		if(curr == 0) {
			conf.set("input_dir", "/Users/twishanaik/Desktop/Rutgers_Sem2/MDM/Homeworks/homework2/data/hw2-q4-kmeans/c2.txt");
		}
		else {
			conf.set("input_dir", "/Users/twishanaik/Desktop/Rutgers_Sem2/MDM/Homeworks/homework2/res_" + curr + ".txt");
		}
		conf.set("output_dir", "/Users/twishanaik/Desktop/Rutgers_Sem2/MDM/Homeworks/homework2/res_" + next + ".txt");
		conf.set("cost_dir", "/Users/twishanaik/Desktop/Rutgers_Sem2/MDM/Homeworks/homework2/cost.txt");

		// Job the job parameters
		job.setJarByClass(Kmeans.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1] + curr));

		job.waitForCompletion(true);

		return 0;
	}
	
	// Initialize the variable for cost
	private static double cost;
	
	// Map class
	public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> {
		// List of centroids read from the file
		List<Double[]> centroids = new ArrayList<Double[]>();
		
		protected void setup(Context context) throws IOException, InterruptedException{
			cost = 0;
			// read file from input directory set in the context
			String input_dir = context.getConfiguration().get("input_dir");
			File file = new File(input_dir);
			int count_centroids = 0;
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new FileReader(file));
				String tmp_line = null;
				while ((tmp_line = reader.readLine()) != null) {
					String line = tmp_line;
					String[] centroid_string = line.trim().split("\\s");
					
					// Convert the string array to double array
					Double[] centroid_double = new Double[centroid_string.length];
					for(int i = 0; i < centroid_string.length; i++) {
						centroid_double[i] = Double.parseDouble(centroid_string[i]);
					}
					
					// Once the centroid is read, add it to the list
					centroids.add(centroid_double);
					System.out.println("Centroid: " + count_centroids++); 
					System.out.println(line);
				}
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				// No matter error occurs or not, close the reader if not null
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e_reader) {
						System.out.println("Error in closing the reader");
					}
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException, InterruptedException{
			// Write the cost after each iteration to the cost file
			String cost_dir = context.getConfiguration().get("cost_dir");
			try{
				FileWriter cost_writer = new FileWriter(cost_dir, true);
				cost_writer.write(cost + "\n");
				cost_writer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String point_line = value.toString();
			String[] point_string = point_line.trim().split("\\s");
			Double[] point = new Double[point_string.length];
			for(int i = 0; i < point_string.length; i++) {
				point[i] = Double.parseDouble(point_string[i]);
			}
			
			double min_distance = Double.MAX_VALUE;
			int centroid_index = -1;
			
			// Compute distance from each of the centroids
			for(int i = 0; i < centroids.size(); i++) {
				double euclidean_dist = get_euclidean_distance(point, centroids.get(i));

				if(euclidean_dist < min_distance) {
					min_distance = euclidean_dist;
					centroid_index = i;
				}
			}
			System.out.println("Centroid index: " + centroid_index + "Distance: " + min_distance);
			context.write(new IntWritable(centroid_index), new Text(point_line));
			cost += min_distance;
		}
	}
	
	public static double get_euclidean_distance(Double[] p1, Double[] p2) {
		double distance = 0;
		if(p1.length != p2.length) {
			throw new RuntimeException("Length of vectors not same");
		}
		for(int i=0; i<p1.length; i++) {
			distance += (p1[i] - p2[i]) * (p1[i] - p2[i]);
		}
		return distance;
	}
	
	// Reduce class
	public static class Reduce extends Reducer<IntWritable, Text, Text, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {   	  
			// Number of features
			int n = 58;
			double[] sum = new double[n];
			int count = 0;

			for(Text value : values) {
				count++;
				String value_str = value.toString();
				String[] point_str = value_str.trim().split("\\s");
				for(int i = 0; i < n; i++) {
					sum[i] += Double.parseDouble(point_str[i]);
				}
			}

			double[] avg = new double[n];
			for(int i=0; i<n; i++) {
				avg[i] = sum[i]/count;
			}
			String result = Double.toString(avg[0]);
			for(int i=1; i<n; i++) {
				result = result + " " + Double.toString(avg[i]);
			}
			System.out.println("Centroid avg: " + result);
			context.write(new Text(result), new Text(""));

			String fileName = context.getConfiguration().get("output_dir");
			try{
				FileWriter output_writer = new FileWriter(fileName, true);
				output_writer.write(result + "\n");
				output_writer.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
		}
	}	
}
