package com.fianlProject.Top20;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class TopKMapper  extends Mapper<Object, Text, NullWritable, Text>{
	private TreeMap<Integer, Text> tumorsizeToRecordMap = new TreeMap<Integer, Text>();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] fields = value.toString().split(",");
		String patient_id = fields[0];
		String tumor_size = fields[5];
		
		
		tumorsizeToRecordMap.put(Integer.parseInt(tumor_size),new Text( value));
		
		if (tumorsizeToRecordMap.size() > 20) {
			tumorsizeToRecordMap.remove(tumorsizeToRecordMap.firstKey());
		}
		
		
	}
	
	@Override
	protected void cleanup(Context context) throws IOException,
	InterruptedException {
		for (Text t : tumorsizeToRecordMap.values()) {
			context.write(NullWritable.get(), t);
		}
	}

}


