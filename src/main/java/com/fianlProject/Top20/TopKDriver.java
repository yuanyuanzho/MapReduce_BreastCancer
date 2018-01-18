package com.fianlProject.Top20;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TopKDriver {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);

		if (args.length != 2) {
			System.err.println("Usage: TopTenDriver <in> <out>");
			System.exit(2);
		}

		Job job = new Job(conf, "Top Ten Users by Reputation");
		job.setJarByClass(TopKDriver.class);
		
		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output))
			hdfs.delete(output, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
