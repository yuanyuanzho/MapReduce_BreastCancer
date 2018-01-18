package com.finalProject.SecondarySort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SecondaryDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Job job = Job.getInstance(conf, "Count Diagnosis");

		job.setJarByClass(SecondaryDriver.class);
		job.setMapperClass(SecondarySortMapper.class);
		job.setMapOutputKeyClass(CompositeKeyWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setSortComparatorClass(SecondarySortCompKeySortComparator.class);
		job.setPartitionerClass(NaturalKeyPartitioner.class);

		job.setReducerClass(SecondarySortReducer.class);
		
		job.setNumReduceTasks(2);
		
	
		// output
		job.setOutputKeyClass(CompositeKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, input);
//		job.setInputFormatClass(TextInputFormat.class);
		
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output))
			hdfs.delete(output, true);
		
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
