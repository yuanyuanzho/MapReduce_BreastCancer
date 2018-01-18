package com.finalProject.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AgeCountNumReducer  extends Reducer<Text, Text, Text, IntWritable>{
	public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		int count = 0;
		String[] arr = null;
		for(Text val: values){
			arr = val.toString().split(",");
			count = arr.length;
		}
		
		context.write(key, new IntWritable(count));
	}
}
