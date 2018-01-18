package com.finalProject.InvertedIndex;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class InvertedAgeMapper extends Mapper<Object, Text, Text, Text>{
	private String[] age_range = {"1-10","10-20","20-30","30-40", "40-50","50-60","60-70","70-80","80-90"};
	private Text outvalue = new Text();
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException{
		String[] fields = value.toString().split(",");
		int age = Integer.parseInt(fields[3]);
		String patient_id = fields[0];
		String diagnosis = fields[7];
		
		if(age > 0 && age <10){
			outvalue.set(patient_id);
			context.write(new Text(age_range[0]), outvalue);
		} else if(age >= 10 && age < 20){
			outvalue.set(patient_id);
			context.write(new Text(age_range[1]), outvalue);
		}else if(age >= 20 && age < 30){
			outvalue.set(patient_id);
			context.write(new Text(age_range[2]), outvalue);
		}else if(age >= 30 && age < 40){
			outvalue.set(patient_id);
			context.write(new Text(age_range[3]), outvalue);
		}else if(age >= 40 && age < 50){
			outvalue.set(patient_id);
			context.write(new Text(age_range[4]), outvalue);
		}else if(age >= 50 && age < 60){
			outvalue.set(patient_id);
			context.write(new Text(age_range[5]), outvalue);
		}else if(age >= 60 && age < 70){
			outvalue.set(patient_id);
			context.write(new Text(age_range[6]), outvalue);
		}else if(age >= 70 && age < 80){
			outvalue.set(patient_id);
			context.write(new Text(age_range[7]), outvalue);
		}else if(age >= 80 && age < 90){
			outvalue.set(patient_id);
			context.write(new Text(age_range[8]), outvalue);
		}
	}

}
