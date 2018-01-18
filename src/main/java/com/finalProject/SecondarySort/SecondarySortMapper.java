package com.finalProject.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SecondarySortMapper extends Mapper<Object, Text, CompositeKeyWritable, NullWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String values[] = value.toString().split(",");
		System.out.println(value.toString());
		int id =0;
		float perimeter =0;
		String diagnosis;
		try {
			id = Integer.parseInt(values[0]);
			perimeter = Float.valueOf(values[4]);
		} catch (Exception e) {
			
		}


			CompositeKeyWritable cw = new CompositeKeyWritable(id, perimeter);

			try {
				context.write(cw, NullWritable.get());
			} catch (Exception e) {
				System.out.println(cw);
				System.out.println(values[8]);
				System.out.println("" + e.getMessage());

			}


	}

}