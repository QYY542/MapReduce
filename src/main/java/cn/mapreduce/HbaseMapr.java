package cn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseMapr {
    /**
     * 创建hbase配置
     */
    static Configuration config = null;
    static {
        config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "shizhan3,shizhan5,shizhan6");
        config.set("hbase.zookeeper.property.clientPort", "2183");
    }
    /**
     * 表信息
     */
    public static final String tableName = "invertedindex";//表名1
    public static final String colf1 = "word";//列族
    public static final String colf2 = "file";//列族
    public static final String colf3 = "line";//列族
    public static final String col = "word";//列
    /**
     * 初始化表结构，及其数据
     */
    public static void initTB() {
        HTable table=null;
        HBaseAdmin admin=null;
        try {
            admin = new HBaseAdmin(config);//创建表管理
            /*删除表*/
            if (admin.tableExists(tableName)) {
                System.out.println("table is already exists!");
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            /*创建表*/
            HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
            HColumnDescriptor family1 = new HColumnDescriptor("word");
            HColumnDescriptor family2 = new HColumnDescriptor("file");
            HColumnDescriptor family3 = new HColumnDescriptor("line");
            desc.addFamily(family1);
            desc.addFamily(family2);
            desc.addFamily(family3);
            admin.createTable(desc);

            /*插入数据*/
            table = new HTable(config,TableName.valueOf(tableName));
            table.setAutoFlush(false);
            table.setWriteBufferSize(500);
            List<Put> lp = new ArrayList<Put>();
            Put p1 = new Put(Bytes.toBytes("1"));
            p1.add(colf1.getBytes(), col.getBytes(),    ("The Apache Hadoop software library is a framework").getBytes());
            lp.add(p1);
            Put p2 = new Put(Bytes.toBytes("2"));p2.add(colf1.getBytes(),col.getBytes(),("The common utilities that support the other Hadoop modules").getBytes());
            lp.add(p2);
            Put p3 = new Put(Bytes.toBytes("3"));
            p3.add(colf1.getBytes(), col.getBytes(),("Hadoop by reading the documentation").getBytes());
            lp.add(p3);
            Put p4 = new Put(Bytes.toBytes("4"));
            p4.add(colf1.getBytes(), col.getBytes(),("Hadoop from the release page").getBytes());
            lp.add(p4);
            Put p5 = new Put(Bytes.toBytes("5"));
            p5.add(colf1.getBytes(), col.getBytes(),("Hadoop on the mailing list").getBytes());
            lp.add(p5);
            table.put(lp);
            table.flushCommits();
            lp.clear();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table!=null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    /**
     * MyMapper 继承 TableMapper
     * TableMapper<Text,IntWritable>
     * Text:输出的key类型，
     * IntWritable：输出的value类型
     */

//    public static class MyMapper extends TableMapper<Text, IntWritable> {
//        private static IntWritable one = new IntWritable(1);
//        private static Text word = new Text();
//        @Override
//        //输入的类型为：key：rowKey； value：一行数据的结果集Result
//        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
//            //获取一行数据中的colf：col
//            String words = Bytes.toString(value.getValue(Bytes.toBytes(colf), Bytes.toBytes(col)));// 表里面只有一个列族，所以我就直接获取每一行的值
//            //按空格分割
//            String itr[] = words.toString().split(" ");
//            //循环输出word和1
//            for (int i = 0; i < itr.length; i++) {
//                word.set(itr[i]);
//                context.write(word, one);
//            }
//        }
//    }
    /**
     * MyReducer 继承 TableReducer
     * TableReducer<Text,IntWritable>
     * Text:输入的key类型，
     * IntWritable：输入的value类型，
     * ImmutableBytesWritable：输出类型，表示rowkey的类型
     */
    public static class MyReducer extends
            TableReducer<Text, IntWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                              Context context) throws IOException, InterruptedException {
            //对mapper的数据求和
            int sum = 0;
            for (IntWritable val : values) {//叠加
                sum += val.get();
            }
            // 创建put，设置rowkey为单词
            Put put = new Put(Bytes.toBytes(key.toString()));
            // 封装数据
            put.add(Bytes.toBytes(colf1), Bytes.toBytes(col),Bytes.toBytes(String.valueOf(sum)));
            //写到hbase,需要指定rowkey、put
            context.write(new ImmutableBytesWritable(Bytes.toBytes(key.toString())),put);
        }
    }
//
//    public static void main(String[] args) throws IOException,
//            ClassNotFoundException, InterruptedException {
//        //初始化表
//        initTB();//初始化表
//        //创建job
//        Job job = new Job((JobConf) config);//job
//        job.setJarByClass(HBaseMr.class);//主类
//        //创建scan
//        Scan scan = new Scan();
//        //可以指定查询某一列
//        scan.addColumn(Bytes.toBytes(colf), Bytes.toBytes(col));
//        //创建查询hbase的mapper，设置表名、scan、mapper类、mapper的输出key、mapper的输出value
//        TableMapReduceUtil.initTableMapperJob(tableName, scan, MyMapper.class,Text.class, IntWritable.class, job);
//        //创建写入hbase的reducer，指定表名、reducer类、job
//        TableMapReduceUtil.initTableReducerJob(tableName2, MyReducer.class, job);
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
}
