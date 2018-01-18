package com.fianlProject.Top20;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TopKReducer  extends Reducer<NullWritable, Text, NullWritable, Text>{
	private TreeMap<Integer, Text> tumorsizeToRecordMap = new TreeMap<Integer, Text>();
	
	public void reduce(NullWritable key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException {
		for(Text val : values){
			try{
			String fields[] = val.toString().split(",");
			Integer tumor_size = Integer.parseInt(fields[5]);
			tumorsizeToRecordMap.put(tumor_size, new Text(val));
			
			if(tumorsizeToRecordMap.size() > 20){
				tumorsizeToRecordMap.remove(tumorsizeToRecordMap.firstKey());
			}
			}catch(NumberFormatException e){
				;
			}
		}
		
		// emit final top 20 list
		for(Text text : tumorsizeToRecordMap.values()){
			context.write(NullWritable.get(), text);
		}
	}
}
