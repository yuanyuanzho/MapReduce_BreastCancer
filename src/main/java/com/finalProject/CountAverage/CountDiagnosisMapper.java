package com.finalProject.CountAverage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class CountDiagnosisMapper extends

	Mapper<Object, Text, Text, AverageComposite> {
	
	private Text outkey = new Text();
	private AverageComposite outvalue = new AverageComposite();
	
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(",");
		String diagnosis = fields[1];
		float radius = Float.valueOf(fields[2]);
		float texture = Float.valueOf(fields[3]);
		float perimeter = Float.valueOf(fields[4]);
		float area = Float.valueOf(fields[5]);
		
		outvalue.setRadius(radius);
		outvalue.setTexture(texture);
		outvalue.setPerimeter(perimeter);
		outvalue.setArea(area);
		outvalue.setCount(1);

		if(diagnosis.equals("B")){
			outkey.set(diagnosis + " Benign: ");
			context.write(outkey, outvalue);
		}else{
			outkey.set(diagnosis + " Malignant: ");
			context.write(outkey,outvalue);
		}
	}
}
