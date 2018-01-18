package com.finalProject.Partitioning;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class PartitionMapper extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(",");
		String state = fields[4];
		
		context.write(new Text(state), value);
	}
}
