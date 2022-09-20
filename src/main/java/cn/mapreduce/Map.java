package cn.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Map extends Mapper<LongWritable, Text, Text, Text> {
    //      word 用来储存单词和URI one 用来储存词频
    private static Text word = new Text();
    private static Text one = new Text();

    private static String num = new String();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
//          获取当前Split下的文件名称
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//            StringTokenizer 是用来把字符串截取成一个个标记或单词的
        StringTokenizer st = new StringTokenizer(value.toString());

        String num = st.nextToken();
        int i = 0;
        while (st.hasMoreTokens()) {
            i++;
            String position = "(" + i + "," + num + ")";
            word.set(st.nextToken() + ":" + fileName + ":" + position);
            context.write(word, one);
        }
    }

}
