package cn.mapreduce;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Reduce extends Reducer<Text, Text, Text, Text> {
    private static StringBuilder sub = new StringBuilder(256);
    private static Text word = new Text();
    private static Text index = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        String[] split = key.toString().split(":");
        int count = 0;
        for (Text v : values) {
            count++;
            sub.append(v.toString());
        }
        word.set(split[0]);
        index.set(split[1] + ":" + count + ":" + sub.toString());
        context.write(word, index);
        sub.delete(0, sub.length());
    }
}
