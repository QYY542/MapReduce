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

class Reduce extends Reducer<Text, Text, Text, Text> {
    private static StringBuilder sub = new StringBuilder(256);
    private static Text index = new Text();

    protected void reduce(Text word, Iterable<Text> values, Context context)
            throws java.io.IOException, InterruptedException {
        for (Text v : values) {
            sub.append(v.toString()).append(";");
        }
        index.set(sub.toString());
        context.write(word, index);
        sub.delete(0, sub.length());
    }

    ;
}
