package com.finalProject.SecondarySort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKeyWritable implements Writable,WritableComparable<CompositeKeyWritable> {

	
	private Integer id;
	private Float perimeter;
	
	
	
	public CompositeKeyWritable(){
		
	}
	
	
	public CompositeKeyWritable(int d,float l){
		this.id=d;
		this.perimeter=l;
		
		
	}
	
	public int compareTo(CompositeKeyWritable o) {
		int result =id.compareTo(o.id);
		if (result==0){
			result=perimeter.compareTo(o.perimeter);
		}
		
		return result;
		
	}

	
	public void write(DataOutput d) throws IOException {
		WritableUtils.writeVInt(d, id);
		WritableUtils.writeString(d,String.valueOf( perimeter));
		
	}

 
	public void readFields(DataInput di) throws IOException {
		id=WritableUtils.readVInt(di);
		perimeter=Float.valueOf(WritableUtils.readString(di));
		
	}



	public Integer getId() {
		return id;
	}


	public void setId(Integer id) {
		this.id = id;
	}


	public Float getPerimeter() {
		return perimeter;
	}


	public void setPerimeter(Float perimeter) {
		this.perimeter = perimeter;
	}


	public String toString(){
		return (new StringBuilder().append(id).append("\t").append(perimeter).toString());
	}
	
	
	

}
