package com.finalProject.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class RadiusCountNumReducer extends Reducer<Text, Text, Text, Text>{
	private Text outvalue = new Text();
	public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		int count_b = 0;
		int count_m = 0;
		String[] s = null;
		for(Text val : values){
			s = val.toString().split(",");
			for(int i = 0; i < s.length; i++){
				if(s[i].equals("B")) count_b++;
				else count_m++;
			}

		}
		StringBuilder sb = new StringBuilder();
		sb.append("B: ").append(count_b).append("\t").append("M: ").append(count_m);
		outvalue.set(sb.toString());
		context.write(key, outvalue);

	}
}


