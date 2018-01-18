package com.finalProject.Join;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class JoinPatientMapper extends Mapper<Object, Text, Text, Text> {
	private Text outKey = new Text();
	private Text outValue = new Text();
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		String stateName = fields[4];
		
		outKey.set(stateName);
		outValue.set("P" + value.toString());
		context.write(outKey, outValue);
		
	}
}
