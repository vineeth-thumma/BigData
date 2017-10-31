package cse.osu.edu.BigDataTwo;
import java.util.*; 
import java.io.IOException; 

import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.conf.*; 
import org.apache.hadoop.io.*; 
import org.apache.hadoop.mapred.*; 
import org.apache.hadoop.util.*;

public class MapReducePhaseOne {
	static int numberOfReducers=10;
	static int beginCategory;
	static int endCategory;
	
	//Mapper class for MR-1
	public static class MR1_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Tuple2, Tuple2>
	{
		//Map function
		public void map(LongWritable key, Text value, OutputCollector<Tuple2, Tuple2> output, Reporter reporter) throws IOException
		{
			String[] values = value.toString().trim().split("\\s+");
			output.collect(new Tuple2(new LongWritable(Long.parseLong(values[0])), new FloatWritable(Float.parseFloat(values[3]))),
					new Tuple2(new LongWritable(Long.parseLong(values[2])), new FloatWritable(Float.parseFloat(values[3]))));
			
		}
	}
	
	//Reducer class for MR-1
	public static class MR1_Reducer extends MapReduceBase implements Reducer<Tuple2, Tuple2, LongWritable, Tuple1>
	{
		//Reduce function
		public void reduce(Tuple2 key, Iterator <Tuple2> value, OutputCollector<LongWritable, Tuple1> output, Reporter reporter) throws IOException
		{
				long sessionCount=0;
				long totalPageCount=0;
				boolean session=false;
				long userId=key.getV1().get();
				while(value.hasNext()) 
				{	
					Tuple2 tuple = value.next();
					long category=tuple.getV1().get();
					if(!session && category==beginCategory) {
						session=true;
						continue;
					}
					if(session && category !=endCategory) {
						totalPageCount++;
					}
					else if(session && category ==endCategory) {
						sessionCount++;
						session=false;
					}
					
				}
				output.collect(new LongWritable(userId), new Tuple1(new LongWritable(totalPageCount), new LongWritable(sessionCount)));
		}
	}
	
	public static void main(String args[])throws Exception 
	{ 
	   JobConf conf = new JobConf(MapReducePhaseOne.class); 
	   
	   conf.setJobName("WebClickStreamAnalysis-MR1"); 
	   conf.setOutputKeyClass(Tuple2.class);
	   conf.setOutputValueClass(Tuple2.class); 
	   conf.setMapperClass(MR1_Mapper.class); 
	   //conf.setCombinerClass(MR1_Reducer.class); 
	   conf.setReducerClass(MR1_Reducer.class); 
	   conf.setInputFormat(TextInputFormat.class); 
	   conf.setOutputFormat(TextOutputFormat.class); 
	   conf.setNumReduceTasks(numberOfReducers);
	   conf.setPartitionerClass(MR1Partitioner.class);
	   conf.setOutputValueGroupingComparator(MR1Comparator.class);
	      
	   FileInputFormat.setInputPaths(conf, new Path(args[0])); 
	   FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
	   beginCategory = Integer.parseInt(args[2]);
	   endCategory = Integer.parseInt(args[3]);
	     
	   JobClient.runJob(conf); 
	} 
}

