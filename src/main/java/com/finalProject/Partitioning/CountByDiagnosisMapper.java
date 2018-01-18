package com.finalProject.Partitioning;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CountByDiagnosisMapper extends Mapper<Object, Text, CompositeKey, IntWritable>{
	private IntWritable one = new IntWritable(1);
	private CompositeKey outkey = new CompositeKey(); 
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(",");
		String state = fields[4];
		String diagnosis = fields[7];
		outkey.setState(state);
		outkey.setDiagnosis(diagnosis);
		context.write(outkey, one);
		
	}
}
