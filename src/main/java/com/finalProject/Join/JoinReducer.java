package com.finalProject.Join;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


public class JoinReducer extends Reducer<Text, Text, Text, Text>{
	private static final Text EMPTY_TEXT = new Text("");
	private Text tmp = new Text();
	private ArrayList<Text> listA = new ArrayList<Text>();
	private ArrayList<Text> listB = new ArrayList<Text>();
	private String joinType = null;
	
	public void setup(Context context){
		joinType = context.getConfiguration().get("join.type");
	}
	
	public void reduce(Text key, Iterable<Text> values,Context context)
			throws IOException, InterruptedException{
		try{
		listA.clear();
		listB.clear();
		
		while(values.iterator().hasNext()){
			tmp = values.iterator().next();
			if(tmp.charAt(0) == 'P'){
				listA.add(new Text(tmp.toString().substring(1)));
			}else if(tmp.charAt(0) == 'S'){
				listB.add(new Text(tmp.toString().substring(1)));
			}
		}
		
		executeJoinLogic(context);
		} catch(NullPointerException e){
			;
		}
	}
	private void executeJoinLogic(Context context) throws IOException, InterruptedException {
		if (!listA.isEmpty()) {
			for (Text A : listA) {
				if (!listB.isEmpty()) {
					for (Text B : listB) {

						context.write(A, B);

					}
				} else {

					context.write(A, EMPTY_TEXT);

				}

			}
		} 
		
		
	}
}
