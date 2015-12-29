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
import java.math.BigDecimal;
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

public class TradePortfolioHdfsDataVerifierV2 extends Configured implements Tool {

  public static class HdfsDataMapper extends Mapper<Key, Row, Key, TradePortfolioRow> {    
    @Override
    public void map(Key key, Row value, Context context) throws IOException , InterruptedException{           
      try {       
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();  
        int sid=rs.getInt("sid");
        int cid = rs.getInt("cid");        
        Key k = new Key();
        k.setKey(CacheServerHelper.serialize(sid + cid));        
        context.write(k, new TradePortfolioRow(cid, sid, rs.getInt("tid"), rs.getInt("qty"),  rs.getInt("availQty"),  rs.getBigDecimal("subTotal")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends Reducer<Key, TradePortfolioRow, Key, TradePortfolioOutputObject> {
    @Override
    public void reduce(Key key, Iterable<TradePortfolioRow> values, Context context) throws IOException , InterruptedException {            
      try {
        for (TradePortfolioRow portfolio : values) {
          context.write(key, new TradePortfolioOutputObject(portfolio));          
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradePortfolioRow implements Writable  {
    int cid, sid, tid, qty , availQty;
    BigDecimal subTotal;   
    
    public TradePortfolioRow (){
    }
    
    public TradePortfolioRow (int cid, int sid, int tid, int qty, int availQty, BigDecimal subTotal){
      this.cid=cid;
      this.sid=sid;
      this.tid=tid;
      this.qty=qty;
      this.availQty=availQty;
      this.subTotal=subTotal;      
    }
            
    public int getCid() {
      return cid;
    }

    public void setCid(int cid) {
      this.cid = cid;
    }
    
    public int getSid() {
      return sid;
    }

    public void setSid(int sid) {
      this.sid = sid;
    }

    public int getTid() {
      return tid;
    }

    public void setTid(int tid) {
      this.tid = tid;
    }

    public int getQty() {
      return qty;
    }

    public void setQty(int qty) {
      this.qty = qty;
    }

    public int getAvailQty() {
      return availQty;
    }

    public void setAvailQty(int availQty) {
      this.availQty = availQty;
    }

    public BigDecimal getSubTotal() {
      return subTotal;
    }

    public void setSubTotal(BigDecimal subTotal) {
      this.subTotal = subTotal;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      System.out.println("writing Portfolio cid: " + cid + " sid: " + sid + " qty: " + qty + " availQty: " + availQty + " subTotal: " + subTotal.toPlainString()  + " tid: " +  tid);
      out.writeInt(cid);
      out.writeInt(sid);
      out.writeInt(tid);
      out.writeInt(qty);
      out.writeInt(availQty);
      out.writeUTF(subTotal.toPlainString());
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      cid= in.readInt();
      sid = in.readInt();
      tid=in.readInt();
      qty=in.readInt();
      availQty=in.readInt();
      subTotal=new BigDecimal (in.readUTF());
    }
  }

  public static class TradePortfolioOutputObject  {
    int cid, sid, tid, qty , availQty;
    BigDecimal subTotal;     
    
    public TradePortfolioOutputObject (int cid, int sid, int tid, int qty, int availQty, BigDecimal subTotal){
      this.cid=cid;
      this.sid=sid;
      this.tid=tid;
      this.qty=qty;
      this.availQty=availQty;
      this.subTotal=subTotal;      
    }           
    
    public TradePortfolioOutputObject(TradePortfolioRow row) {
      this.cid=row.cid;
      this.sid=row.sid;
      this.tid=row.tid;
      this.qty=row.qty;
      this.availQty=row.availQty;
      this.subTotal=row.subTotal;
    }
    
    public void setCid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,cid);
    }    
    
    public void setSid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,sid);
    }
    
    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }

    public void setQty(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,qty);
    }

    public void setAvailQty(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,availQty);
    }

    public void setSubTotal(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,subTotal);
    }
    
  }

  public int run(String[] args) throws Exception {
    GfxdDataSerializable.initTypes();

    Configuration conf = getConf();
    
    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("TradePortfolioHdfsDataVerifierV2.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    
    
    Job job = Job.getInstance(conf, "TradePortfolioHdfsDataVerifierV2");
    job.setJobName("TradePortfolioHdfsDataVerifierV2");
    job.setInputFormatClass(RowInputFormat.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    
      
    job.setMapperClass(HdfsDataMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(TradePortfolioRow.class);   
    
    job.setReducerClass(HdfsDataReducer.class);  
    job.setOutputKeyClass(Key.class);
    job.setOutputValueClass(TradePortfolioOutputObject.class);
    
    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());
    
    return job.waitForCompletion(false) ? 0 : 1;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradePortfolioHdfsDataVerifierV2.main() invoked with " + args);    
    int rc = ToolRunner.run(new Configuration(),new TradePortfolioHdfsDataVerifierV2(), args);
    System.exit(rc);
  }
}
