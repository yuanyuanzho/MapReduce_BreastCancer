package com.finalProject.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RadiusCountNumMapper extends Mapper<Object, Text, Text, Text>{
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(":");
		String outkey = fields[0];
		String outvalue = fields[1];
		
		context.write(new Text(outkey), new Text(outvalue));
	}
}


