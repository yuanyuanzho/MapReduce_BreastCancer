package com.finalProject.InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedRadiusReducer extends Reducer<Text, Text, Text, Text>{
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values,Context context) 
			throws IOException, InterruptedException{
		StringBuilder sb = new StringBuilder();
		ArrayList<String> lists = new ArrayList<String>();
		boolean first = true;
		
		for(Text radius : values){
			lists.add(radius.toString());
        }
		
		for(String s : lists){
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
