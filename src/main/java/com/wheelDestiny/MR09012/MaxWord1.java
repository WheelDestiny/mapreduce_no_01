/**
 * Distict.java
 * com.hainiu.day03
 * Copyright (c) 2020, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.MR09012;

import com.wheelDestiny.MR09012.Base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实现求全局最大值单词
 */
public class MaxWord1 extends BaseMR {

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskId());

		job.setJarByClass(com.wheelDestiny.MR0901101.MaxWord1.class);

		job.setMapperClass(com.wheelDestiny.MR0901101.MaxWord1.MaxWord1Mapper.class);
		job.setReducerClass(com.wheelDestiny.MR0901101.MaxWord1.MaxWord1Reducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		//创建上一个任务的实例
		WordCount wordCount = new WordCount();

		//上一个任务的输出目录
		FileInputFormat.addInputPath(job,wordCount.getJobOutputPath(wordCount.getJobNameWithTaskId()));

		FileOutputFormat.setOutputPath(job,getJobOutputPath(getJobNameWithTaskId()));

		return job;
	}

	@Override
	public String getJobName() {
		return "maxWord";
	}

	public static class MaxWord1Mapper extends Mapper<LongWritable, Text, Text, LongWritable>{
	
		/**
		 * map端局部最大值的单词
		 */
		Text keyOut = new Text();
		
		/**
		 * map端局部最大值封装的对象
		 */
		LongWritable valueOut = new LongWritable();
		/**
		 * map端局部最大值
		 */
		long maxNum = Long.MIN_VALUE;
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("\t");
			// 防御式编程
			if(arr.length != 2){
				context.getCounter("hainiu", "bad line num").increment(1L);
				return;
			}
			
			String word = arr[0];
			long num = Long.parseLong(arr[1]);
			
			// 通过这样的方式获取最大值和单词
			if(maxNum < num){
				keyOut.set(word);
				maxNum = num;
			}
				
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// 通过cleanup 数据map端局部最大值
			valueOut.set(maxNum);
			context.write(keyOut, valueOut);
			System.out.println("mapper max ==> key:" + keyOut.toString() + ", value:" + maxNum);
			
		}
	}
	
	public static class MaxWord1Reducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		/**
		 * reducer端全局最大值的单词
		 */
		Text keyOut = new Text();
		
		/**
		 * reducer端全局最大值封装的对象
		 */
		LongWritable valueOut = new LongWritable();
		/**
		 * reducer端全局最大值
		 */
		long maxNum = Long.MIN_VALUE;
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			
			StringBuilder sb = new StringBuilder();
			sb.append("reduce ==>");
			sb.append("key:" + key.toString() + ", ");
			sb.append("values=[");
			// 求全局最大值
			for(LongWritable w : values){
				long num = w.get();
				sb.append(num).append(",");
				if(maxNum < num){
					maxNum = num;
					keyOut.set(key.toString());
				}
			}
			sb.deleteCharAt(sb.length() - 1);
			sb.append("]");
			
			System.out.println(sb.toString());
			
		}
		
		@Override
		protected void cleanup(Context context)
				throws IOException, InterruptedException {
			// 输出全局最大值
			valueOut.set(maxNum);
			context.write(keyOut, valueOut);
			System.out.println("reducer max ==> key:" + keyOut.toString() + ", value:" + maxNum);
			
		}
	}


}

