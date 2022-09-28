package cn.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


public class HbaseUtils {
    Configuration conf;
    public HbaseUtils(Configuration conf) {
        this.conf = conf;
    }
    public void createTable(String tableName) throws IOException {
        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();
        if(admin.tableExists(TableName.valueOf(tableName)))
        {
            System.out.println("表已存在");
            admin.disableTable(TableName.valueOf(tableName));
            admin.deleteTable(TableName.valueOf(tableName));
        }
        //  表描述构造器
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
        //  列族描述器构造器
        ColumnFamilyDescriptorBuilder col1 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("items"));
        ColumnFamilyDescriptorBuilder col2 = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("counts"));
        //  列描述器
        ColumnFamilyDescriptor build1 = col1.build();
        ColumnFamilyDescriptor build2 = col2.build();
        //  添加列簇
        TableDescriptorBuilder builder = tableDescriptorBuilder.setColumnFamily(build1).setColumnFamily(build2);
        //  表描述器
        TableDescriptor tableDescriptor = builder.build();
        //  创建表
        admin.createTable(tableDescriptor);
        admin.close();
        conn.close();
        System.out.println("create table success!");
    }

}
