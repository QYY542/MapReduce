package cn.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce_v2 extends Reducer<Text, Text, Text, Text> {
    private static StringBuilder sub = new StringBuilder();
    private static Text word = new Text();
    private static Text index = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        for (Text value : values) {
            sub.append(value).append(";");
        }
        word.set(key);
        index.set(sub.toString());
        context.write(word, index);
        sub.delete(0, sub.length());
    }
}
