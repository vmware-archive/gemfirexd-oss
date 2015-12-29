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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.hadoop.mapreduce.Key;
import com.pivotal.gemfirexd.hadoop.mapreduce.Row;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowOutputFormat;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;

public class TradesHdfsDataVerifierV2 extends Configured implements Tool {

  public static class HdfsDataMapper extends  Mapper<Key, Row, Key, TradesRow> {    
    @Override
    public void map(Key key, Row value, Context context) throws IOException , InterruptedException{           
      try {         
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();
        int tid=rs.getInt("tid");
        Key k = new Key();
        k.setKey(CacheServerHelper.serialize(tid));  
        context.write(k, new TradesRow(rs.getInt("cid"), tid, rs.getInt("eid"), rs.getDate("tradeDate")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends Reducer<Key, TradesRow, Key, TradeOutputObject> {
    @Override
    public void reduce(Key key, Iterable<TradesRow> values,Context context) throws IOException , InterruptedException {            
      try {
        for  ( TradesRow trade : values) {
          context.write(key, new TradeOutputObject(trade));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradesRow implements Writable  {
    int cid, tid, eid;
    Date tradeDate;;   
    
    
    public TradesRow (){
    }
    
    public TradesRow (int cid, int tid, int eid , Date tradeDate){
      this.cid=cid;
      this.tid=tid;
      this.eid=eid;
      this.tradeDate=tradeDate;      
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

    public int getEid() {
      return eid;
    }

    public void setEid(int eid) {
      this.eid = eid;
    }

    public Date getTradeDate() {
      return tradeDate;
    }

    public void setTradeDate(Date tradeDate) {
      this.tradeDate = tradeDate;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      System.out.println("writing Trades cid:" + cid + " tid: " + tid + " eid: " + eid + " tradeDate: " + tradeDate.getTime());
      out.writeInt(cid);
      out.writeInt(tid);
      out.writeInt(eid);
      out.writeLong(tradeDate.getTime());
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      cid = in.readInt();
      tid=in.readInt();
      eid=in.readInt();
      tradeDate=new Date(in.readLong());
    }
  }

  public static class TradeOutputObject  {
    int cid, tid, eid;
    Date tradeDate;
    
    public TradeOutputObject (int cid, int tid, int eid , Date tradeDate){
      this.cid=cid;
      this.tid=tid;
      this.eid=eid;
      this.tradeDate=tradeDate;  
    }
       

    public TradeOutputObject(TradesRow row) {
      this.cid=row.cid;
      this.tid=row.tid;
      this.eid=row.eid;
      this.tradeDate=row.tradeDate; 
    }
    
    public void setCid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,cid);
    }


    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }


    public void setEid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,eid);
    }


    public void setTradeDate(int i, PreparedStatement ps) throws SQLException  {
      ps.setDate(i,tradeDate);
    }      
    
  }

  public int run(String[] args) throws Exception {
    GfxdDataSerializable.initTypes();

    Configuration conf = getConf();
    
    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("TradesHdfsDataVerifier.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    
    
    Job job = Job.getInstance(conf, "TradesHdfsDataVerifierV2");
    job.setJobName("TradesHdfsDataVerifierV2");
    job.setInputFormatClass(RowInputFormat.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    
      
    job.setMapperClass(HdfsDataMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(TradesRow.class);   
    
    job.setReducerClass(HdfsDataReducer.class);  
    job.setOutputKeyClass(Key.class);
    job.setOutputValueClass(TradeOutputObject.class);
    
    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());
    
    return job.waitForCompletion(false) ? 0 : 1;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradesHdfsDataVerifierV2.main() invoked with " + args);    
    int rc = ToolRunner.run(new Configuration(),new TradesHdfsDataVerifierV2(), args);
    System.exit(rc);
  }
}
