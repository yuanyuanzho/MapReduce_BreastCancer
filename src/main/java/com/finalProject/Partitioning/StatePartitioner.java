package com.finalProject.Partitioning;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class StatePartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numPartitions) {
		String state = key.toString();
		if(state.startsWith("A")){
			return 0;
		}else if(state.startsWith("C")){
			return 1;
		}else if(state.startsWith("D")){
			return 2;
		}else if(state.startsWith("F")){
			return 3;
		}else if(state.startsWith("G")){
			return 4;
		}else if(state.startsWith("H")){
			return 5;
		}else if(state.startsWith("I")){
			return 6;
		}else if(state.startsWith("K")){
			return 7;
		}else if(state.startsWith("L")){
			return 8;
		}else if(state.startsWith("M")){
			return 9;
		}else if(state.startsWith("N")){
			return 10;
		}else if(state.startsWith("O")){
			return 11;
		}else if(state.startsWith("P")){
			return 12;
		}else if(state.startsWith("R")){
			return 13;
		}else if(state.startsWith("S")){
			return 14;
		}else if(state.startsWith("T")){
			return 15;
		}else if(state.startsWith("U")){
			return 16;
		}else if(state.startsWith("V")){
			return 17;
		}else if(state.startsWith("W")){
			return 18;
		}
		
		return 1;
	}

}
