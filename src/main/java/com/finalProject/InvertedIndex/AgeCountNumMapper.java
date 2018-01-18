package com.finalProject.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class AgeCountNumMapper extends Mapper<Object, Text, Text, Text>{
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split("\\s+");
		String age_range = fields[0];
		String values = fields[1];
		
		context.write(new Text(age_range), new Text(values));
	}
}
