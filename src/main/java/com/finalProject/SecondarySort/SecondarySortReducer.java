package com.finalProject.SecondarySort;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
 
public class SecondarySortReducer extends
        Reducer<CompositeKeyWritable, NullWritable, CompositeKeyWritable, NullWritable> {
	

										// unique key
	  public void reduce(CompositeKeyWritable key,Iterable<NullWritable> values,Context context)
	            throws IOException, InterruptedException {
		
		  for(NullWritable value :values){
			  try {
				  
				  context.write(key, NullWritable.get());
			} catch (Exception e) {
				
			}
		  }
		
	       
	    }
}



