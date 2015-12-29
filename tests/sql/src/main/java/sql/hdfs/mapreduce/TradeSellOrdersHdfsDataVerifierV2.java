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
import java.sql.Timestamp;

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



public class TradeSellOrdersHdfsDataVerifierV2 extends Configured implements Tool {

  public static class HdfsDataMapper extends  Mapper<Key, Row, Key, TradeSellOrdersRow> {
    public void map(Key key, Row value, Context context) throws IOException , InterruptedException {           
      try {        
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();
        int oid=rs.getInt("oid");
        Key k = new Key();
        k.setKey(CacheServerHelper.serialize(oid));
        context.write(k, new TradeSellOrdersRow(oid, rs.getInt("cid"), rs.getInt("sid"), rs.getInt("qty"),  rs.getString("status"),  rs.getBigDecimal("ask"), rs.getTimestamp("order_time") , rs.getInt("tid")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends  Reducer<Key, TradeSellOrdersRow, Key, TradeSellOrdersOutputObject> {
    public void reduce(Key key, Iterable<TradeSellOrdersRow> values,Context context) throws IOException , InterruptedException{            
      try {
        for (TradeSellOrdersRow sellorder : values) {
          context.write(key, new TradeSellOrdersOutputObject(sellorder));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradeSellOrdersRow implements Writable  {   
    int oid, cid, sid, qty , tid ;
    String status;
    BigDecimal ask;   
    Timestamp orderTime;
    
    public TradeSellOrdersRow (){
    }
    
    public TradeSellOrdersRow (int oid, int cid, int sid, int qty, String status, BigDecimal ask, Timestamp orderTime , int tid){
      this.oid=oid;
      this.cid=cid;
      this.sid=sid;
      this.qty=qty;
      this.status=status;
      this.ask=ask;
      this.orderTime=orderTime;
      this.tid = tid;
    }
         
    public int getOid() {
      return oid;
    }

    public void setOid(int oid) {
      this.oid = oid;
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

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public BigDecimal getAsk() {
      return ask;
    }

    public void setAsk(BigDecimal ask) {
      this.ask = ask;
    }

    public Timestamp getOrderTime() {
      return orderTime;
    }

    public void setOrderTime(Timestamp orderTime) {
      this.orderTime = orderTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {   
      System.out.println("writing Sellorders oid: " + oid + " cid: " + cid + " sid: " + sid + " qty: " + qty + " status: " + status + " ask: " + ask.toPlainString() + " orderTime: " + orderTime.getTime() + " tid: " +  tid);
      out.writeInt(oid);
      out.writeInt(cid);
      out.writeInt(sid);
      out.writeInt(qty);
      out.writeUTF(status);
      out.writeUTF(ask.toPlainString());      
      out.writeLong(orderTime.getTime());
      out.writeInt(tid);
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      oid=in.readInt();
      cid= in.readInt();
      sid = in.readInt();
      qty=in.readInt();
      status=in.readUTF();
      ask=new BigDecimal(in.readUTF());
      orderTime=new Timestamp(in.readLong());
      tid=in.readInt();
    }
  }

  public static class TradeSellOrdersOutputObject  {
    int oid, cid, sid, qty, tid ;
    String status;
    BigDecimal ask;   
    Timestamp orderTime;
    
    public TradeSellOrdersOutputObject (int oid, int cid, int sid, int qty,  String status, BigDecimal ask, Timestamp orderTime ,int tid){
      this.oid=oid;
      this.cid=cid;
      this.sid=sid;
      this.qty=qty;
      this.status=status;
      this.ask=ask;
      this.orderTime=orderTime;
      this.tid=tid;
    }       
    
    public TradeSellOrdersOutputObject(TradeSellOrdersRow row) {
      this.oid=row.oid;
      this.cid=row.cid;
      this.sid=row.sid;
      this.qty=row.qty;
      this.status=row.status;
      this.ask=row.ask;
      this.orderTime=row.orderTime;
      this.tid=row.tid;
    }
    
    public void setOid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,oid);
    }    
    public void setCid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,cid);
    }    
    
    public void setSid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,sid);
    }
    
    public void setQty(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,qty);
    }
    
    public void setStatus(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,status);
    }

    public void setAsk(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,ask);
    }
    
    public void setOrder_Time(int i, PreparedStatement ps) throws SQLException  {
      ps.setTimestamp(i,orderTime);
    }
    
    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }    
  }

  public int run(String[] args) throws Exception {
    GfxdDataSerializable.initTypes();

    Configuration conf = getConf();
    
    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("TradeSellOrdersHdfsDataVerifier.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    
    
    Job job = Job.getInstance(conf, "TradeSellOrdersHdfsDataVerifierV2");
    job.setJobName("TradeSellOrdersHdfsDataVerifierV2");
    job.setInputFormatClass(RowInputFormat.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    
      
    job.setMapperClass(HdfsDataMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(TradeSellOrdersRow.class);   
    
    job.setReducerClass(HdfsDataReducer.class);  
    job.setOutputKeyClass(Key.class);
    job.setOutputValueClass(TradeSellOrdersOutputObject.class);
    
    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());
    
    return job.waitForCompletion(false) ? 0 : 1;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradeSellOrdersHdfsDataVerifierV2.main() invoked with " + args);    
    int rc = ToolRunner.run(new Configuration(),new TradeSellOrdersHdfsDataVerifierV2(), args);
    System.exit(rc);
  }
}
