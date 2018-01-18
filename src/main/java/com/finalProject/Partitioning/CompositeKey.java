package com.finalProject.Partitioning;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;



public class CompositeKey implements Writable, WritableComparable<CompositeKey>{
	private String state;
	private String diagnosis;
	

	public CompositeKey() {
	}

	public CompositeKey(String state, String diagnosis) {
		super();
		this.state = state;
		this.diagnosis = diagnosis;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getDiagnosis() {
		return diagnosis;
	}

	public void setDiagnosis(String diagnosis) {
		this.diagnosis = diagnosis;
	}

	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, state);
		WritableUtils.writeString(out, diagnosis);
		
	}

	public void readFields(DataInput in) throws IOException {
		state = WritableUtils.readString(in);
		diagnosis = WritableUtils.readString(in);
		
	}

	public String toString(){
		return new StringBuffer().append(state).append("\t").append(diagnosis).toString();
		
	}
	public int compareTo(CompositeKey o) {
		int result =state.compareTo(o.state);
		if (result==0){
			result=diagnosis.compareTo(o.diagnosis);
		}
		
		return result;
	}
}
