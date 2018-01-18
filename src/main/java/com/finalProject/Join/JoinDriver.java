package com.finalProject.Join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class JoinDriver {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Inner Join");
		Path patient_input = new Path(args[0]);
		Path state_input = new Path(args[1]);
		Path join_output = new Path(args[2]);
		
		job.setJarByClass(JoinPatientMapper.class);
		
		// Multiple inputs
		MultipleInputs.addInputPath(job, patient_input, TextInputFormat.class, JoinPatientMapper .class);
		MultipleInputs.addInputPath(job, state_input, TextInputFormat.class, JoinStateMapper.class);
		job.getConfiguration().set("join.type", "Inner");

		job.setReducerClass(JoinReducer.class);
		job.setNumReduceTasks(1);
		
	

		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setOutputPath(job, join_output);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(join_output))
			hdfs.delete(join_output, true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
