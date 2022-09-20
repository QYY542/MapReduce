package cn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * 使用经典方法提交MapReduce作业
 */
public class InvertedIndex_v1 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //job1的配置
        Job job1 = Job.getInstance(conf, "Job1");
        job1.setJarByClass(InvertedIndex_v1.class);
        job1.setMapperClass(Map.class);
        job1.setCombinerClass(Combine.class);
        job1.setReducerClass(Reduce.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileSystem fs = new Path(args[1]).getFileSystem(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        job1.setMaxMapAttempts(4);
//        job1.setNumReduceTasks(50);

        /*
         * job1的输出路径是job2的输入路径
         * 判断job1结束的返回状态，成功结束就执行job2
         * job2只是依赖job1的结果路径，并不是依赖job1的输出结果的键值对类型。
         */

        if (job1.waitForCompletion(true)) {
            //job2的配置
            Job job2 = Job.getInstance(conf, "Job2");
            job2.setJarByClass(InvertedIndex_v1.class);
            job2.setMapperClass(Map_v2.class);
            job2.setReducerClass(Reduce_v2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            if (fs.exists(new Path(args[2]))) {
                fs.delete(new Path(args[2]), true);
            }
            job2.setMaxMapAttempts(4);
//            job2.setNumReduceTasks(50);
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
