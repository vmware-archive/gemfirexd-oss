/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package sql.hdfs;

import com.pivotal.gemfirexd.hadoop.mapred.RowInputFormat;
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;
import com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hydra.Log;

import java.io.IOException;
import java.io.DataOutput;
import java.io.DataInput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class VerifyHdfsDataUsingMR extends Configured implements Tool {

  public static class HdfsDataMapper extends MapReduceBase implements Mapper<Key, Row, Text, MyRow> {

    @Override
    public void map(Key key, Row value, OutputCollector<Text, MyRow> output, Reporter reporter) throws IOException {      
      String tableName = null;
      try {
        ResultSet rs = value.getRowAsResultSet();
        tableName = rs.getMetaData().getTableName(1);
        
        Log.getLogWriter().info("i am in a mapper and table Name is " + tableName);
        
        int cid = rs.getInt("cid");
        String cname = rs.getString("cust_name");
        String addr = rs.getString("addr");
        int tid = rs.getInt("tid");
          
        Log.getLogWriter().info("mapper procesing record from " + tableName + ": " + cid + ": " + cname + ": "  + addr + ": " + tid);                 
         
        Text myKey = new Text(Integer.toString(cid));
        MyRow myRow = new MyRow (cid, cname ,  addr , tid);
        Log.getLogWriter().info("MAPPER writing intermediate record " + myRow.toString());
        
        output.collect(myKey, myRow);
        
      } catch (SQLException se) {
        System.err.println("Error logging result set" + se);
      }
    }
  }

  public static class HdfsDataReducer extends MapReduceBase implements Reducer<Text, MyRow, Key, DataObject> {

    @Override
    public void reduce(Text key, Iterator<MyRow> values, OutputCollector<Key, DataObject> output, Reporter reporter) throws IOException {
      Log.getLogWriter().info("i am in reducer");
      while (values.hasNext()) {
        MyRow myRow = values.next();
        int cid = myRow.getCid();
        int tid = myRow.getTid();
        String addr = myRow.getAddr();
        String cname = myRow.getCname();
        Log.getLogWriter().info("reducer processing record for " + myRow.toString());   
        DataObject o = new DataObject(cid, cname, addr, tid);
        Log.getLogWriter().info("reducer writing record " + o.toString());
        output.collect(new Key(), o);
      }     
      Log.getLogWriter().info("i am in out from reducer");
    }
  }

  public static class MyRow implements Writable {
    private int cid;
    private int tid;
    private String addr;
    private String cname;
    
    
    public MyRow (int cid, String cname , String addr , int tid){
      this.cid=cid;
      this.cname=cname;
      this.addr=addr;
      this.tid=tid;
    }
    public int getCid() {
      return cid;
    }

    public void setCid(int cid) {
      this.cid = cid;
    }

    public int getTid() {
      return tid;
    }

    public void setTid(int tid) {
      this.tid = tid;
    }

    public String getAddr() {
      return addr;
    }

    public void setAddr(String addr) {
      this.addr = addr;
    }

    public String getCname() {
      return cname;
    }

    public void setCname(String cname) {
      this.cname = cname;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(cid);
      out.writeChars(cname);
      out.writeChars(addr);
      out.writeInt(tid);
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      cid = in.readInt();
      cname=in.readLine();
      addr=in.readLine();
      tid=in.readInt();
    }
  }

  public static class DataObject {
    int cid;
    int tid;
    String cname;
    String addr;

    public DataObject(int cid, String cname, String addr, int tid) {
     this.cid=cid;
     this.cname=cname;
     this.addr=addr;
     this.tid=tid;
    }

    public int getCid() {
      return cid;
    }

    public void setCid(int cid) {
      this.cid = cid;
    }

    public int getTid() {
      return tid;
    }

    public void setTid(int tid) {
      this.tid = tid;
    }

    public String getCname() {
      return cname;
    }

    public void setCname(String cname) {
      this.cname = cname;
    }

    public String getAddr() {
      return addr;
    }

    public void setAddr(String addr) {
      this.addr = addr;
    }


  }

  public int run(String[] args) throws Exception {

    // todo@lhughes -- why do we need this?
    GfxdDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("hdfsMapReduce");

    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("VerifyHdfsData.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    
    conf.setInputFormat(RowInputFormat.class);
    conf.setMapperClass(HdfsDataMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(MyRow.class);
    
    conf.setReducerClass(HdfsDataReducer.class);
    conf.set(RowOutputFormat.OUTPUT_TABLE, "TRADE.HDFS_CUSTOMERS");
    //conf.set(GfxdOutputFormat.OUTPUT_SCHEMA, "APP");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    conf.setOutputFormat(RowOutputFormat.class);
    conf.setOutputKeyClass(Key.class);
    conf.setOutputValueClass(DataObject.class);

    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());

    // not planning to use this, but I get an NPE without it
    FileOutputFormat.setOutputPath(conf, new Path("" + System.currentTimeMillis()));
    
    JobClient.runJob(conf);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("VerifyHdfsData.main() invoked with " + args);    
    int rc = ToolRunner.run(new VerifyHdfsDataUsingMR(), args);
    System.exit(rc);
  }
}
