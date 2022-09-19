package cn.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.util.StringTokenizer;

public class Map_v2 extends Mapper<LongWritable, Text, Text, Text> {
    //      word 用来储存单词和URI one 用来储存词频
    private static Text word = new Text();
    private static Text one = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
//          获取当前Split下的文件名称
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
//            StringTokenizer 是用来把字符串截取成一个个标记或单词的
        StringTokenizer st = new StringTokenizer(value.toString());
//        st.nextToken();
        one.set(st.nextToken());
        while (st.hasMoreTokens()) {
            word.set(st.nextToken() + ":" + fileName);
            context.write(word, one);
        }
    }
}
