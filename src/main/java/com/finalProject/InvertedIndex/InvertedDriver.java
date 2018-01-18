package com.finalProject.InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.counters.Limits;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;






public class InvertedDriver 
{
	public static void main( String[] args ) throws Exception
    {
    	Configuration conf = new Configuration(true);

		Path input_wdbs = new Path(args[0]);
		Path input_patient = new Path(args[1]);
		Path output_radius_inverted = new Path(args[2]);
		Path output_age_inverted = new Path(args[3]);
		Path output_age_inverted_count = new Path(args[4]);
		Path output_radius_inverted_count = new Path(args[5]);



		Job job = new Job(conf, "Inverted Index");
		
		job.setJarByClass(InvertedDriver.class);

		job.setMapperClass(InvertedRadiusMapper.class);
		job.setReducerClass(InvertedRadiusReducer.class);
		job.setNumReduceTasks(1);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);


		FileInputFormat.addInputPath(job, input_wdbs);
		FileOutputFormat.setOutputPath(job, output_radius_inverted);
		

		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output_radius_inverted))
			hdfs.delete(output_radius_inverted, true);
		


		boolean complete1 = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Inverted Index");
		
		
		if (complete1) {
			job2.setJarByClass(InvertedDriver.class);
			job2.setMapperClass(InvertedAgeMapper.class);
			job2.setReducerClass(InvertedAgeReducer.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job2, input_patient);
			FileOutputFormat.setOutputPath(job2, output_age_inverted);
			
			if (hdfs.exists(output_age_inverted))
				hdfs.delete(output_age_inverted, true);
		}
	

		boolean complete2 = job2.waitForCompletion(true);

		Configuration conf3 = new Configuration();
		Job job3 = Job.getInstance(conf3, "Inverted Index");
	
		if (complete2) {
			job3.setJarByClass(InvertedDriver.class);
			job3.setMapperClass(AgeCountNumMapper.class);
			job3.setReducerClass(AgeCountNumReducer.class);
			
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(Text.class);
			job3.setNumReduceTasks(1);

			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job3, output_age_inverted);
			FileOutputFormat.setOutputPath(job3, output_age_inverted_count);
			
			
			if (hdfs.exists(output_age_inverted_count))
				hdfs.delete(output_age_inverted_count, true);
			

		}
		

		boolean complete3 = job3.waitForCompletion(true);

		Configuration conf4 = new Configuration();
		Job job4 = Job.getInstance(conf4, "Inverted Index");

		if (complete3) {
			job4.setJarByClass(InvertedDriver.class);
			job4.setMapperClass(RadiusCountNumMapper.class);
			job4.setReducerClass(RadiusCountNumReducer.class);
			
			job4.setMapOutputKeyClass(Text.class);
			job4.setMapOutputValueClass(Text.class);
			job4.setNumReduceTasks(1);

			job4.setOutputKeyClass(Text.class);
			job4.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job4, output_radius_inverted);
			FileOutputFormat.setOutputPath(job4, output_radius_inverted_count);
			
			
			if (hdfs.exists(output_radius_inverted_count))
				hdfs.delete(output_radius_inverted_count, true);
			
			System.exit(job4.waitForCompletion(true) ? 0 : 1);
		}
		
		
    }
}
