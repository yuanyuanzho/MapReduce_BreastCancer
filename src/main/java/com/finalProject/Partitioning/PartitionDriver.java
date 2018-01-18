package com.finalProject.Partitioning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.finalProject.InvertedIndex.InvertedAgeMapper;
import com.finalProject.InvertedIndex.InvertedAgeReducer;
import com.finalProject.InvertedIndex.InvertedDriver;

public class PartitionDriver {
	public static void main( String[] args ) throws Exception
    {
		Configuration conf = new Configuration();

        Path input = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        Path input_count = outputDir;
        Path output_count = new Path(args[2]);
        
        Job job = new Job(conf, "Partitioner Pattern");
        
        job.setJarByClass(PartitionMapper.class);
        job.setMapperClass(PartitionMapper.class);
        job.setCombinerClass(PartitionReducer.class);
        job.setReducerClass(PartitionReducer.class);
        job.setPartitionerClass(StatePartitioner.class);
        
        job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        job.setNumReduceTasks(19);
        
        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, outputDir);
        
        FileSystem hdfs = FileSystem.get(conf);
        if(hdfs.exists(outputDir)) hdfs.delete(outputDir, true);
        if(hdfs.exists(output_count)) hdfs.delete(output_count, true);
        
		boolean complete1 = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Inverted Index");
		
		// only work if complete is true
		if (complete1) {
			job2.setJarByClass(PartitionDriver.class);
			job2.setMapperClass(CountByDiagnosisMapper.class);
			job2.setReducerClass(CountByDiagnosisReducer.class);
			
			job2.setMapOutputKeyClass(CompositeKey.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setNumReduceTasks(1);


			job2.setOutputKeyClass(CompositeKey.class);
			job2.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job2, input_count);
			FileOutputFormat.setOutputPath(job2, output_count);
			

			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
    }
}
