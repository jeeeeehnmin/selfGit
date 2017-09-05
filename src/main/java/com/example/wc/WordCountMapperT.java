package com.example.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapperT extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	private final static IntWritable one = new IntWritable(1);
	private final Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split(",");
		
		for(String v : line){
			word.set(v);
			context.write(word, one);
		}
		
	}

}
