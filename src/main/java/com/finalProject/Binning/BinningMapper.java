package com.finalProject.Binning;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class BinningMapper extends Mapper<Object, Text, Text, NullWritable>{
	
	private MultipleOutputs<Text, NullWritable> mos = null;
	
	protected void setup(Context context){
		mos = new MultipleOutputs(context);
	}
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(",");
		String outcome =fields[1];
		
		if(outcome.equals("N")){
			mos.write("bins",value,NullWritable.get(), "N");
		}else if(outcome.equals("R")){
			mos.write("bins",value,NullWritable.get(), "R");
		}
	}
	
	protected void cleanup(Context context) throws IOException, InterruptedException{
		mos.close();
	}
	
}


