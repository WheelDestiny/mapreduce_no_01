package com.wheelDestiny.MapJoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MJMapper extends Mapper<LongWritable, Text,Text, NullWritable> {

    private Map<String,String> map = new HashMap();
    private Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] uris = context.getCacheFiles();
        String path = uris[0].getPath().toString();

        //此处开流可以选择HDFS的流，但是HDFS的流对换行的处理非常差，使用readLine又会有编码问题

        BufferedReader bufferedReader = new BufferedReader(new FileReader(path));
        String line;
        while (StringUtils.isNotEmpty(line = bufferedReader.readLine())){
            String[] files = line.split("\t");
            map.put(files[0],files[1]);
        }
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] files = value.toString().split("\t");
        String pname = map.get(files[1]);
        k.set(files[0]+"\t"+pname+"\t"+files[2]);
        context.write(k,NullWritable.get());
    }
}
