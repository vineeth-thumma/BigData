package cse.osu.edu.BigDataTwo;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class MapReducePhaseTwo {
	//Mapper class for MR-2
		public static class MR2_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Tuple1>
		{
			//Map function
			public void map(LongWritable key, Text value, OutputCollector<LongWritable, Tuple1> output, Reporter reporter) throws IOException
			{
				String[] values = value.toString().trim().split("\\s+");
				output.collect(new LongWritable(Long.parseLong("1")),
						new Tuple1(new LongWritable(Long.parseLong(values[1])), new LongWritable(Long.parseLong(values[2]))));
				
			}
		}
		
		//Reducer class for MR-2
		public static class MR2_Reducer extends MapReduceBase implements Reducer<LongWritable, Tuple1, Text, Text>
		{
			//Reduce function
			public void reduce(LongWritable key, Iterator <Tuple1> value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
			{
					long sessionCount=0;
					long totalPageCount=0;
					
					while(value.hasNext()) 
					{	
						Tuple1 tuple = value.next();
						sessionCount += tuple.getV1().get();
						totalPageCount += tuple.getV2().get();
						
					}
					float answer = (float)sessionCount/totalPageCount;
					output.collect(new Text(""), new Text(String.valueOf(answer)));
			}
		}
		
		public static void main(String args[])throws Exception 
		{ 
		   JobConf conf = new JobConf(MapReducePhaseOne.class); 
		   
		   conf.setJobName("WebClickStreamAnalysis-MR2"); 
		   conf.setOutputKeyClass(LongWritable.class);
		   conf.setOutputValueClass(Tuple1.class); 
		   conf.setMapperClass(MR2_Mapper.class); 
		   conf.setReducerClass(MR2_Reducer.class); 
		   conf.setInputFormat(TextInputFormat.class); 
		   conf.setOutputFormat(TextOutputFormat.class); 
		   conf.setNumReduceTasks(1);
		   		      
		   FileInputFormat.setInputPaths(conf, new Path(args[0])); 
		   FileOutputFormat.setOutputPath(conf, new Path(args[1])); 
		   		     
		   JobClient.runJob(conf); 
		} 
}
