package com.finalProject.CountAverage;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class CountDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Path input = new Path(args[0]);
		Path output = new Path(args[1]);
		
		Job job = Job.getInstance(conf, "Count Diagnosis");
		job.setJarByClass(CountDriver.class);
		job.setMapperClass(CountDiagnosisMapper.class);
		job.setReducerClass(CountDiagnosisReducer.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(AverageComposite.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AverageComposite.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(output))
			hdfs.delete(output, true);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}
