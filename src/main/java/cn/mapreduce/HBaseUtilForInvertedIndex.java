package cn.mapreduce;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.*;
//import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;


public class HBaseUtilForInvertedIndex {

    Configuration conf;
    Connection conn;
    public HBaseUtilForInvertedIndex() {
        //创建连接对象
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "node1:2181,node2:2181,node3:2181");
        try {
            assert conf != null;
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public  void createTable(String tableName) throws Exception {
        Admin admin = conn.getAdmin();
        TableName tbName = TableName.valueOf(tableName);
        if(admin.tableExists(tbName)){
            admin.disableTable(tbName);
            admin.deleteTable(tbName);
        }
        HTableDescriptor tableDescriptor = new HTableDescriptor(tbName);// 表的数据模式
        tableDescriptor.addFamily (new HColumnDescriptor("word"));// 增加列族
        tableDescriptor.addFamily(new HColumnDescriptor("filename"));
        tableDescriptor.addFamily(new HColumnDescriptor("linenumber"));
        admin.createTable(tableDescriptor);
    }
    public void showCellData(String tableName, String rowKey, String family, String qualifier) throws Exception {
        Table table = getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        get.addColumn(family.getBytes(), qualifier.getBytes());
        Result result = table.get(get);
        showData(result);
    }

    public void showRowData(String tableName, String rowKey) throws Exception {
        Table table = getTable(tableName);
        Get get = new Get(rowKey.getBytes());
        //get.setMaxVersions();显示所有版本
        //get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        showData(result);
        for(Cell cell : result.rawCells()){
            System.out.println(" 行 键 :"  + Bytes.toString(result.getRow()));
            System.out.println(" 列 族 "  + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :"  +  Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :"  + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }
    private  Admin getAdmin() throws Exception {
        return conn.getAdmin();
    }
    private Table getTable(String tableName) throws Exception {

        TableName tbName = TableName.valueOf(tableName);
        return conn.getTable(tbName);
    }

    private  Connection getConn() {
        return conn;
    }

    public void addData(String tableName, Put put) throws Exception {
        getTable(tableName).put(put);
    }

    public void addData(String tableName, String rowKey, String family, String qualifier, String value)
            throws Exception {
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
        addData(tableName, put);
    }

    public void addData(String tableName, String rowKey, String family, String qualifier, int timestamp, String value)
            throws Exception {
        Put put = new Put(rowKey.getBytes());
        put.addColumn(family.getBytes(), qualifier.getBytes(), timestamp, value.getBytes());
        addData(tableName, put);
    }

    public void deleteData(String tableName, Delete delete) throws Exception {
        getTable(tableName).delete(delete);
    }

    public void deleteData(String tableName, String rowKey, String family, String qualifier) throws Exception {
        Delete delete = new Delete(rowKey.getBytes()).addColumn(family.getBytes(), qualifier.getBytes());
        deleteData(tableName, delete);
    }



    private void showData(Result result) {

        while (result.advance()) {
            Cell cell = result.current();
            System.out.println(" 行 键 :"  + Bytes.toString(result.getRow()));
            System.out.println(" 列 族 "  + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println(" 列 :"  +  Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println(" 值 :"  + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());

            String row = Bytes.toString(CellUtil.cloneRow(cell));
            String cf = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
            String val = Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(row + "--->" + cf + "--->" + qualifier + "--->" + val);
        }
    }

    public static void main(String[] args) {
        HBaseUtilForInvertedIndex hbaseUtil = new HBaseUtilForInvertedIndex();
        try {
            hbaseUtil.createTable("invertedindex");
            System.out.println("OK");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}