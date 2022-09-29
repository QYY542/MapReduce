package cn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.MapReduceExtendedCell;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 使用工具类ToolRunner提交MapReduce作业
 * 需要在pom.xml里修改主类
 */

public class InvertedIndex extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        new HbaseUtils(conf).createTable("invertedindex");
        int status = ToolRunner.run(conf, new InvertedIndex(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        //job1的配置
        args = new String[]{"/hxh/bit/input","/hxh/bit/output", "/hxh/bit/finaloutput"};
        Job job1 = Job.getInstance(getConf(), "Job1");
        job1.setJarByClass(InvertedIndex.class);
        //-job1的Mapper、Combiner、Reducer
        job1.setMapperClass(Map_v1.class);
        job1.setCombinerClass(Combine_v1.class);
        job1.setReducerClass(Reduce_v1.class);
        //-job1的MapOutputKey、MapOutputValue、OutputKey、OutputValue
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        //-job1的输入输出路径
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        //-job1运行时删除已存在的文件夹
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        job1.setMaxMapAttempts(4);
        job1.setNumReduceTasks(10);

        /*
         * job1的输出路径是job2的输入路径
         * 判断job1结束的返回状态，成功结束就执行job2
         * job2只是依赖job1的结果路径，并不是依赖job1的输出结果的键值对类型。
         */

        if (job1.waitForCompletion(true)) {
            //job2的配置
            Job job2 = Job.getInstance(getConf(), "Job2");
            job2.setJarByClass(InvertedIndex.class);
            //-job2的Mapper、Reducer
            job2.setMapperClass(Map_v2.class);
            job2.setReducerClass(Reduce_v2.class);
            //-job2的MapOutputKey、MapOutputValue、OutputKey、OutputValue
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(ImmutableBytesWritable.class);
            job2.setOutputValueClass(MapReduceExtendedCell.class);
            //-job2的输入输出路径
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            if (fs.exists(new Path(args[2]))) {
                fs.delete(new Path(args[2]), true);
            }
            job2.setMaxMapAttempts(4);
            job2.setNumReduceTasks(10);
            //job2运行结束后结束程序
//            System.exit(job2.waitForCompletion(true) ? 0 : 1);
            TableMapReduceUtil.initTableReducerJob("invertedindex", Reduce_v2.class, job2);
            return job2.waitForCompletion(true) ? 0 : 1;
        }

        return job1.waitForCompletion(true) ? 0 : 1;
    }
}
