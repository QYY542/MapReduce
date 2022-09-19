package cn.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

public class Reduce_HBase extends TableReducer<Text, Text, Text> {
    private static StringBuilder sub = new StringBuilder(256);
    private static Text index = new Text();

    public Put outputValue;

    protected void reduce(Text word, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        for (Text v : values) {
            sub.append(v.toString()).append(";");
        }
        index.set(sub.toString());
        outputValue = new Put(Bytes.toBytes(index.toString()));
        context.write(word, outputValue);
        sub.delete(0, sub.length());
    }
}
