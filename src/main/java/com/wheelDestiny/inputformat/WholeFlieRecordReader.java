package com.wheelDestiny.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
* 自定义RR，处理一个文件，把这个文件直接读成一个K，V值
* */
public class WholeFlieRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;

    private FileSplit fileSplit;

    /**
     * 初始化方法，框架会在开始的时候调用一次
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //转换切片类型到文件切片
        fileSplit = (FileSplit)split;
        //通过切片获取路径
        Path path = fileSplit.getPath();
        //通过路径获取文件系统
        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        //开启流
        inputStream = fileSystem.open(path);
    }

    /**
     * 尝试读取读取下一个K,V值，如果读到了返回true，读完返回false
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (notRead) {
            //具体读文件的过程
            //读Key  文件路径
            key.set(fileSplit.getPath().toString());

            //读value    文件内容
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            inputStream.read(bytes);
            value.set(bytes,0,bytes.length);

            notRead = false;
            return true;
        }else {
            //读过了
            return false;
        }
    }

    /**
     * 获取当前读到的Key
     * @return 当前key，如果没有当前Key，返回null
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    /**
     * 获取当前读到的Value
     * @return 当前value
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    /**
     *返回当前数据读取的进度(0.0-1.0)
     * @return 当前进度
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return notRead?0:1;
    }

    /**
     *关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        //关闭流
        IOUtils.closeStream(inputStream);
    }
}
