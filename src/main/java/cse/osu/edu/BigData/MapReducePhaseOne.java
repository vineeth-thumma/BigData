package cse.osu.edu.BigData;
import java.util.*; 
import java.io.IOException; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*; 

public class MapReducePhaseOne {
	//Mapper class for MR-1
	public static class MR1_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, LongWritable>
	{
		//Map function
		public void map(LongWritable key, Text value, OutputCollector<LongWritable, LongWritable> output, Reporter reporter) throws IOException
		{
			String number = value.toString().trim();
			output.collect(new LongWritable(Long.parseLong(number)), new LongWritable(Long.parseLong("1")));
			
		}
	}
	
	//Reducer class for MR-1
	public static class MR1_Reducer extends MapReduceBase implements Reducer<LongWritable, LongWritable, LongWritable, LongWritable>
	{
		//Reduce function
		public void reduce(LongWritable key, Iterator <LongWritable> value, OutputCollector<LongWritable, LongWritable> output, Reporter reporter) throws IOException
		{
				long sum=0;
				while(value.hasNext()) 
				{
					sum += value.next().get();
				}
				output.collect(key, new LongWritable(sum));
		}
	}
	
	public static void main(String args[])throws Exception 
	{ 
	   JobConf conf = new JobConf(MapReducePhaseOne.class); 
	   
	   conf.setJobName("NumberRanking-MR1"); 
	   conf.setOutputKeyClass(LongWritable.class);
	   conf.setOutputValueClass(LongWritable.class); 
	   conf.setMapperClass(MR1_Mapper.class); 
	   conf.setCombinerClass(MR1_Reducer.class); 
	   conf.setReducerClass(MR1_Reducer.class); 
	   conf.setInputFormat(TextInputFormat.class); 
	   conf.setOutputFormat(TextOutputFormat.class); 
	      
	   FileInputFormat.setInputPaths(conf, new Path(args[0])); 
	   FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
	     
	   JobClient.runJob(conf); 
	} 
}