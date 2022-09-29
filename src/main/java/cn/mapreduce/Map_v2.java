package cn.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.StringTokenizer;

public class Map_v2 extends Mapper<LongWritable, Text, Text, Text> {
    //      word 用来储存单词和URI one 用来储存词频
    private static Text word = new Text();
    private static Text one = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws java.io.IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString());
        if (st.hasMoreTokens()) {
            word.set(st.nextToken());
        }
        if (st.hasMoreTokens()) {
            one.set(st.nextToken());
        }
        //<word,(fileName:count:position)>
        context.write(word, one);
    }
}
