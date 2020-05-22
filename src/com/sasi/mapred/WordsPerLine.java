package com.sasi.mapred;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WordsPerLine
{
	public static class MapperClass extends Mapper<LongWritable,Text, Text, IntWritable>
	{
		int lineno = 1;
		public void map(LongWritable key, Text value,Context con) throws IOException, InterruptedException
		{
			String line = value.toString();
            StringTokenizer token = new StringTokenizer(line);
            while(token.hasMoreTokens())
            {
            	String word = token.nextToken();
            	String l = "line"+lineno;   
            	Text outputKey = new Text(l);
            	IntWritable outputValue = new IntWritable(1);
            	con.write(outputKey, outputValue);
            }
            lineno++;
          } // end of map()
	 }
	
	public static class ReducerClass extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values,Context con) throws IOException, InterruptedException
		{
			int sum=0;
			for(IntWritable value: values)
				sum+=value.get();
			 con.write(key , new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception{
		Configuration conf=new Configuration();
		Job job=new Job(conf,"WordsPerLine");
		
		job.setJarByClass(WordsPerLine.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(MapperClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
