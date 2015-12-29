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
package sql.hdfs.mapreduce;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

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

import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;
import com.pivotal.gemfirexd.hadoop.mapred.RowInputFormat;
import com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;

public class TradeCustomersHdfsDataVerifier extends Configured implements Tool {

  public static class HdfsDataMapper extends MapReduceBase implements Mapper<Key, Row, Text, TradeCustomersRow> {
    
    @Override
    public void map(Key key, Row value, OutputCollector<Text, TradeCustomersRow> output, Reporter reporter) throws IOException {           
      try {
          
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();
        int cid=rs.getInt("cid");        
        output.collect(new Text(Integer.toString(cid)), new TradeCustomersRow(cid, rs.getInt("tid"), rs.getString("cust_name"), rs.getString("addr"), rs.getDate("since")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends MapReduceBase implements Reducer<Text, TradeCustomersRow, Key, TradeCustomerOutputObject> {
    @Override
    public void reduce(Text key, Iterator<TradeCustomersRow> values, OutputCollector<Key, TradeCustomerOutputObject> output, Reporter reporter) throws IOException {            
      try {
        while (values.hasNext()) {
          TradeCustomersRow customer = values.next();
          Key k = new Key();
          k.setKey(CacheServerHelper.serialize(customer.getCid()));  
          output.collect(k, new TradeCustomerOutputObject(customer));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradeCustomersRow implements Writable  {
    int cid, tid;
    String cname, addr;
    Date since;;   
    
    
    public TradeCustomersRow (){
    }
    
    public TradeCustomersRow (int cid, int tid, String cname, String addr , Date since){      
      this.cname=cname;
      this.cid=cid;
      this.tid=tid;      
      this.addr=addr;
      this.since=since;      
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

    public Date getSince() {
      return since;
    }

    public void setSince(Date since) {
      this.since = since;
    }

    @Override
    public void write(DataOutput out) throws IOException {
     
      if (cname == null || cname.equals(""))  cname =" ";        
      System.out.println("writing Customer cid: " + cid + " tid: " + tid + " since: " + since.getTime() + " cname: " + cname + " addr: " + addr);
      out.writeInt(cid);
      out.writeInt(tid);
      out.writeLong(since.getTime());
      out.writeUTF(cname);
      out.writeUTF(addr);
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      cid = in.readInt();
      tid=in.readInt();
      since=new Date(in.readLong());
      cname=in.readUTF();      
      addr=in.readUTF();
      cname= cname.equals(" ")?null:cname;
    }
  }

  public static class TradeCustomerOutputObject  {
    int cid, tid;
    String cust_name, addr;
    Date since;
    
    public TradeCustomerOutputObject (int cid, int tid, String cname, String addr , Date since){
      this.cid=cid;
      this.tid=tid;
      this.cust_name=cname;
      this.addr=addr;
      this.since=since;      
    }
       

    public TradeCustomerOutputObject(TradeCustomersRow row) {
      this.cid=row.cid;
      this.tid=row.tid;
      this.cust_name=row.cname;
      this.addr=row.addr;
      this.since=row.since; 
    }
    
    public void setCid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,cid);
    }


    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }


    public void setCust_name(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,cust_name);
    }


    public void setAddr(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,addr);
    }


    public void setSince(int i, PreparedStatement ps) throws SQLException  {
      ps.setDate(i,since);
    }      
    
  }

  public int run(String[] args) throws Exception {

    GfxdDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("TradeCustomersHdfsDataVerifier");

    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("TradeCustomersHdfsDataVerifier.run() invoked with " 
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
    conf.setMapOutputValueClass(TradeCustomersRow.class);
    
    conf.setReducerClass(HdfsDataReducer.class);
    conf.set(RowOutputFormat.OUTPUT_TABLE, tableName + "_HDFS");
    //conf.set(GfxdOutputFormat.OUTPUT_SCHEMA, "APP");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    conf.setOutputFormat(RowOutputFormat.class);
    conf.setOutputKeyClass(Key.class);
    conf.setOutputValueClass(TradeCustomerOutputObject.class);

    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());

    
    FileOutputFormat.setOutputPath(conf, new Path("" + System.currentTimeMillis()));
    
    JobClient.runJob(conf);
    return 0;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradeCustomersHdfsDataVerifier.main() invoked with " + args);    
    int rc = ToolRunner.run(new TradeCustomersHdfsDataVerifier(), args);
    System.exit(rc);
  }
}
