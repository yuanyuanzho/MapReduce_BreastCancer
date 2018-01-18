package com.finalProject.CountAverage;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;


public class AverageComposite implements Writable{
	private float radius;
	private float texture;
	private float perimeter;
	private float area;
	private int count;
	
	public AverageComposite() {
		
	}

	
	public AverageComposite(int count, float radius, float texture, float perimeter, float area) {
		super();
		this.count = count;
		this.radius = radius;
		this.texture = texture;
		this.perimeter = perimeter;
		this.area = area;
	}



	public float getRadius() {
		return radius;
	}


	public void setRadius(float radius) {
		this.radius = radius;
	}


	public float getTexture() {
		return texture;
	}


	public void setTexture(float texture) {
		this.texture = texture;
	}


	public float getPerimeter() {
		return perimeter;
	}


	public void setPerimeter(float perimeter) {
		this.perimeter = perimeter;
	}


	public float getArea() {
		return area;
	}


	public void setArea(float area) {
		this.area = area;
	}


	public int getCount() {
		return count;
	}


	public void setCount(int count) {
		this.count = count;
	}


	
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, String.valueOf(radius));
		WritableUtils.writeString(out, String.valueOf(texture));
		WritableUtils.writeString(out, String.valueOf(perimeter));
		WritableUtils.writeString(out, String.valueOf(area));
		WritableUtils.writeVInt(out, count);
		
	}

	
	public void readFields(DataInput in) throws IOException {
		radius = Float.valueOf(WritableUtils.readString(in));
		texture = Float.valueOf(WritableUtils.readString(in));
		perimeter = Float.valueOf(WritableUtils.readString(in));
		area = Float.valueOf(WritableUtils.readString(in));
		count = WritableUtils.readVInt(in);
	}

	@Override
	public String toString(){
		return new StringBuffer().append(count)
				.append(",  Average Radius: ").append(radius)
				.append(", Average Texture: ").append(texture)
				.append(", Average Perimeter: ").append(perimeter)
				.append(", Average Area:").append(area).toString();
		
	}
	
	
	
}
