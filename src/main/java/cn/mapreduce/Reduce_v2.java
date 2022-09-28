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
        long file_count = 0;
        long total_count = 0;
        for (Text value : values) {
            //index填入
            file_count += 1;
            String[] splits = value.toString().split(":");
            total_count += Long.parseLong(splits[1]);
            sub.append(value).append(";");
        }
        word.set(key);
        index.set(sub.toString());
        //<word,(fileName_1:count_1:position_1);(fileName_2:count_2:position_2)>
//        context.write(word, index);
        Put put = new Put(key.toString().getBytes());
        put.addColumn("items".getBytes(), "item".getBytes(), index.getBytes());
        put.addColumn("counts".getBytes(), "file_count".getBytes(), String.valueOf(file_count).getBytes());
        put.addColumn("counts".getBytes(), "total_count".getBytes(), String.valueOf(total_count).getBytes());
        sub.delete(0, sub.length());
        context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);

    }
}
