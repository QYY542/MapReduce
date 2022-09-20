package cn.mapreduce;

import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class Map_v1 extends Mapper<LongWritable, Text, Text, Text> {
    //word 用来储存单词和URI one 用来储存词频
    private static Text word = new Text();
    //one没有用，如果填入坐标信息就会发生错误，所以需要两个job完成任务
    private static Text one = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        //获取当前Split下的文件名称
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        //StringTokenizer 是用来把字符串截取成一个个标记或单词的
        StringTokenizer st = new StringTokenizer(value.toString());
        //获取句子的序号并且拿掉第一个字符串
        String num = st.nextToken();
        //进行单词位置计数
        int i = 0;
        //判断st是否还有下一个字符串
        while (st.hasMoreTokens()) {
            i++;
            //单词的position
            String position = "(" + num + "," + i + ")";
            word.set(st.nextToken() + ":" + fileName + ":" + position);
            //<(word:fileName:position),one>
            context.write(word, one);
        }
    }

}
