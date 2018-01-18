package com.finalProject.InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedRadiusMapper extends Mapper<Object, Text, Text, Text>{
	private Text range = new Text();
	private Text outkey = new Text();
	private String[] s = new String[] {"0-5mm:", "5-10mm:", "10-15mm:", "15-20mm:", "20-25mm:", "25-30mm:","30-35mm:","35-40mm:"};
	private List<String> ranges = new ArrayList<String>(Arrays.asList(s));
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(",");
		double radius = Double.parseDouble(fields[2]);
		
		if(radius >= 0 && radius < 5){
			String diagnosis = fields[1];
			outkey.set(diagnosis);
			range.set("Radius between "+ ranges.get(0));
			context.write(range, outkey);
		} else if(radius >= 5 && radius < 10){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(1));
			context.write(range, outkey);
		} else if(radius >= 10 && radius < 15){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(2));
			context.write(range, outkey);
		} else if(radius >= 15 && radius < 20){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(3));
			context.write(range, outkey);
		} else if(radius >= 20 && radius < 25){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(4));
			context.write(range, outkey);
		} else if(radius >= 25 && radius < 30){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(5));
			context.write(range, outkey);
		}else if(radius >= 30 && radius < 35){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(6));
			context.write(range, outkey);
		} else if(radius >= 35 && radius < 40){
			String diagnosis = fields[1];
			outkey.set(diagnosis);

			range.set("Radius between "+ ranges.get(7));
			context.write(range, outkey);
		}
	}
}
