package com.finalProject.BloomFilter;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CountIrradiateMapper extends Mapper <LongWritable, Text,Text,IntWritable>{

	private IntWritable one = new IntWritable(1);
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{

		String[] fields = value.toString().split(",");
		String irradiate = fields[9];
		
		context.write(new Text(irradiate), one);
	
	}
}



