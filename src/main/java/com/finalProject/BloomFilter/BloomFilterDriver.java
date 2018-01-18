package com.finalProject.BloomFilter;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.finalProject.InvertedIndex.AgeCountNumMapper;
import com.finalProject.InvertedIndex.AgeCountNumReducer;
import com.finalProject.InvertedIndex.InvertedDriver;


public class BloomFilterDriver {

	public static void main(String[] args) throws Exception{
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "join");
        
        Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        Path output2 = new Path(args[3]);
        
        DistributedCache.addCacheFile(new URI(args[2]), job.getConfiguration());
        job.setJarByClass(BloomFilterDriver.class);
        job.setMapperClass(BloomFilterMapper.class);

        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputDir);

    	// Delete output if exis
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(outputDir))
			hdfs.delete(outputDir, true);
        
//        job.waitForCompletion(true);
		
		// when second job was done.
		boolean complete2 = job.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "Inverted Index");
		
		// only work if complete is true
		if (complete2) {
			job2.setJarByClass(BloomFilterDriver.class);
			job2.setMapperClass(CountIrradiateMapper.class);
			job2.setReducerClass(CountIrradiateReducer.class);
			
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);
			job2.setNumReduceTasks(1);

			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job2, outputDir);
			FileOutputFormat.setOutputPath(job2, output2);
			
			
			if (hdfs.exists(output2))
				hdfs.delete(output2, true);
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		
	}

}
