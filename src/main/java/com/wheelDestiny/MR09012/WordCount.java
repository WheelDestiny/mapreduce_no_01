/**
 * WordCount.java
 * com.hainiu.day01
 * Copyright (c) 2020, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.MR09012;

import com.wheelDestiny.MR09012.Base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实现单词统计
 */
public class WordCount extends BaseMR {

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskId());

		job.setJarByClass(com.wheelDestiny.MR0901101.WordCount.class);
		job.setMapperClass(com.wheelDestiny.MR0901101.WordCount.WordCountMapper.class);

		job.setReducerClass(com.wheelDestiny.MR0901101.WordCount.WordCountReducer.class);
		job.setMapOutputKeyClass(Text.class);

		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.addInputPath(job, getFirstJobInputPath());

		Path outputPath = getJobOutputPath(getJobNameWithTaskId());
		FileOutputFormat.setOutputPath(job, outputPath);

		return job;
	}

	@Override
	public String getJobName() {
		return "wordCount";
	}

	/*
        KEYIN :  每行的起始字节, 是个数值， Hadoop用自带的Writable对象 LongWritable ---> long
        VALUEIN： 每行的数据，是个字符串,  Hadoop用自带的Writable对象 Text ---> String
         比如：
        one piece      0     one piece
        one piece      12	 one piece
        one            24    one
        one            30    one

        -------------------
        根据wordcount的图发现 map() 输出的 是  key是单词， value是数值
        KEYOUT：   key是单词， Text
        VALUEOUT：value是数值， LongWritable
        【重点】map输出的key,value类型 是由 业务决定的
         */
	// 自定义Mapper类
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		
		/**
		 * map 输出的key
		 */
		Text keyOut = new Text();
		
		/**
		 * map 输出的value
		 */
		LongWritable valueOut = new LongWritable(1L);
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 统计输入目录数据行数
			context.getCounter("hainiu", "line num").increment(1L);
			// one piece  -->  one   1
			//                 piece 1
			// 提取出Text里的字符串
			String line = value.toString();
			System.out.println("mapper input key ==> " + key.get() + ", value==>" + value);
			// one piece --> {"one","piece"}
			String[] splits = line.split(" ");
			for(String word : splits){
				// 通过context.write 输出 keyout  和  valueout
				keyOut.set(word);
				context.write(keyOut, valueOut);
			}
		}
	}
	
	
	/*
	 ruduce 的输入类型 和 map输出的类型一致
	 KEYIN：      key是单词， Text
	 VALUEIN：value是数值， LongWritable
	 
	根据wordcount的图发现 reduce() 输出的 是  key是单词， value是数值
	KEYOUT：   key是单词， Text
	VALUEOUT：value是数值， LongWritable
	【重点】reduce输出的key,value类型 是由 业务决定的
	 */
	// 自定义Reducer类
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		
		/**
		 * reduce 输出的key
		 */
		Text keyOut = new Text();
		
		/**
		 * reduce 输出的value
		 */
		LongWritable valueOut = new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			
			// 统计key种类个数
			context.getCounter("hainiu", "key type num").increment(1L);
			// one,[1,1,1,1,1]  --> one, 5
			StringBuilder sb = new StringBuilder();
			sb.append("reduce ==>");
			sb.append("key:" + key.toString() + ", ");
			sb.append("values=[");
			long sum = 0L;
			for(LongWritable w : values){
				long num = w.get();
				sb.append(num).append(",");
				sum += num;
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("]");
			
			System.out.println(sb.toString());
			
			// 通过context.write 输出 keyout  和  valueout
			keyOut.set(key.toString());
			valueOut.set(sum);
			context.write(keyOut, valueOut);
		}
	}
	

}

