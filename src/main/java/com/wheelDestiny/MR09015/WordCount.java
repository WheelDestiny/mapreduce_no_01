/**
 * WordCount.java
 * com.hainiu.day01
 * Copyright (c) 2020, 海牛版权所有.
 * @author   潘牛                      
*/

package com.wheelDestiny.MR09015;

import com.wheelDestiny.MR09012.Base.BaseMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 实现单词统计
 */
public class WordCount extends BaseMR {

	@Override
	public Job getJob(Configuration conf) throws IOException {
		Job job = Job.getInstance(conf, getJobNameWithTaskId());

		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);

		job.setReducerClass(WordCountReducer.class);
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
	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text keyOut = new Text();
		LongWritable valueOut = new LongWritable(1L);
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] splits = line.split(";");
			if(splits[2].equals("boy")){
				keyOut.set(splits[3]+"#男孩");
				context.write(keyOut, valueOut);
			}else {
				keyOut.set(splits[3]+"#女孩");
				context.write(keyOut, valueOut);
			}

		}
	}
	// 自定义Reducer类
	public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
		Text keyOut = new Text();
		LongWritable valueOut = new LongWritable();
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			long sum = 0L;
			for(LongWritable w : values){
				long num = w.get();
				sum += num;
			}
			keyOut.set(key.toString());
			valueOut.set(sum);
			context.write(keyOut, valueOut);
		}
	}
	

}

