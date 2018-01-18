package com.finalProject.SecondarySort;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator{
	
	protected GroupingComparator() {
		
		super(CompositeKeyWritable.class,true);
		
	}
	
	public int compare(WritableComparable w1,WritableComparable w2){
		
		CompositeKeyWritable cw1= (CompositeKeyWritable)w1;
		CompositeKeyWritable cw2= (CompositeKeyWritable)w2;
		return cw1.getId().compareTo(cw2.getId());		
	}

}
