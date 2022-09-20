package cn.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce_v1 extends Reducer<Text, Text, Text, Text> {
    private static StringBuilder sub = new StringBuilder();
    private static Text word = new Text();
    private static Text index = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        String[] split = key.toString().split(":");
        int count = 0;
        for (Text v : values) {
            count++;
            //position填入
            sub.append(v.toString());
        }
        word.set(split[0]);
        index.set(split[1] + ":" + count + ":" + sub.toString());
        //<word,(fileName:count:position)>
        //但是没有将word的每一个(fileName:count:position)合并，需要job2实现
        context.write(word, index);
        sub.delete(0, sub.length());
    }
}
