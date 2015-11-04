package com.skangyam.hadoop.mapreduce.WordFilterStatic;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordFilterStMapper extends Mapper<LongWritable, Text, Text, Text>
{
	@Override
    protected void map(LongWritable key, Text value, Context context) 
    		throws IOException, InterruptedException
    {
		String[] str = value.toString().split(",");
		if (str[1].equalsIgnoreCase("CA")){
			context.write(new Text("CA"), new Text(str[2]));
		}
		
        
    }
}
