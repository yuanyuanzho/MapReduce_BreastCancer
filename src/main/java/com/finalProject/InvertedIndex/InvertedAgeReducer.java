package com.finalProject.InvertedIndex;

import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class InvertedAgeReducer extends Reducer<Text, Text, Text, Text>{
	private Text result = new Text();
	public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		
		StringBuilder sb = new StringBuilder();
		HashSet<String> hashset = new HashSet<String>();
		boolean first = true;
		
		for(Text age : values){
            hashset.add(age.toString());
        }
		
		for(String s : hashset){
			if(first){
				first = false;
			}else{
				sb.append(",");
			}
			sb.append(s.toString());
		}
		
		result.set(sb.toString());
		context.write(key, result);
	}
}
