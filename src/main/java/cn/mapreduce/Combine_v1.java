package cn.mapreduce;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Combine_v1 extends Reducer<Text, Text, Text, Text> {
    private static Text word = new Text();
    private static Text index = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        String[] splits = key.toString().split(":");
        word.set(splits[0] + ":" + splits[1]);
        index.set(splits[2]);
        //<(word:fileName),position>
        context.write(word, index);
    }

}

