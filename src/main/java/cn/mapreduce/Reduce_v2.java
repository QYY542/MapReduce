package cn.mapreduce;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.nio.charset.StandardCharsets;

public class Reduce_v2 extends TableReducer<Text, Text, ImmutableBytesWritable> {
    private static StringBuilder sub = new StringBuilder();
    private static Text word = new Text();
    private static Text index = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        for (Text value : values) {
            //index填入
            sub.append(value).append(";");
        }
        word.set(key);
        index.set(sub.toString());
        //<word,(fileName_1:count_1:position_1);(fileName_2:count_2:position_2)>
//        context.write(word, index);
        Put put = new Put(key.toString().getBytes());
        put.add("items".getBytes(), "filelist".getBytes(), index.getBytes());
        sub.delete(0, sub.length());
        context.write(null, put);

    }
}
