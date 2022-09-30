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

public class SingleJobTwo extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
        new HbaseUtils(conf).createTable("Inv");
        int status = ToolRunner.run(conf, new JobAll(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {

        args = new String[]{"/hxh/bit/output","/hxh/bit/finaloutput"};
        FileSystem fs = FileSystem.get(getConf());
        //job2的配置
        Job job2 = Job.getInstance(getConf(), "Job2");
        job2.setJarByClass(JobAll.class);
        //-job2的Mapper、Reducer
        job2.setMapperClass(Map_v2.class);
        job2.setReducerClass(Reduce_v2.class);
        //-job2的MapOutputKey、MapOutputValue、OutputKey、OutputValue
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(ImmutableBytesWritable.class);
        job2.setOutputValueClass(MapReduceExtendedCell.class);
        //-job2的输入输出路径
        FileInputFormat.addInputPath(job2, new Path(args[0]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        job2.setMaxMapAttempts(4);
        job2.setNumReduceTasks(30);
        TableMapReduceUtil.initTableReducerJob("Inv", Reduce_v2.class, job2);
        return job2.waitForCompletion(true) ? 0 : 1;

    }
}
