package com.finalProject.CountAverage;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class CountDiagnosisReducer extends Reducer<Text, AverageComposite, Text, AverageComposite>{

	public void reduce(Text key, Iterable<AverageComposite> values,Context context) 
			throws IOException, InterruptedException{
		int count=0;
		float radius = 0;
		float texture =0;
		float perimeter = 0;
		float area = 0;
		
		for(AverageComposite value : values){
			radius += value.getCount() * value.getRadius();
			texture += value.getCount() * value.getTexture();
			perimeter += value.getCount() * value.getPerimeter();
			area += value.getCount() * value.getArea();
			count += value.getCount();
		}
		
		AverageComposite result = new AverageComposite(count, radius/count, texture/count, perimeter/count, area/count);
		context.write(key, result);
	}
}
