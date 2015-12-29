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

public class TradeBuyOrdersHdfsDataVerifierV2 extends Configured implements Tool {

  public static class HdfsDataMapper extends  Mapper<Key, Row, Key, TradeBuyOrdersRow> {
    public void map(Key key, Row value, Context context) throws IOException , InterruptedException {           
      try {                
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
            ResultSet rs = value.getRowAsResultSet();
            int oid=rs.getInt("oid");
            Key k = new Key();
            k.setKey(CacheServerHelper.serialize(oid)); 
            context.write(k, new TradeBuyOrdersRow(oid, rs.getInt("cid"), rs.getInt("sid"), rs.getInt("tid"), rs.getInt("qty"),  rs.getString("status"),  rs.getBigDecimal("bid"), rs.getTimestamp("ordertime")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends Reducer<Key, TradeBuyOrdersRow, Key, TradeBuyOrdersOutputObject> {
    public void reduce(Key key, Iterable<TradeBuyOrdersRow> values, Context context) throws IOException,InterruptedException  {            
      try {
        for (TradeBuyOrdersRow buyorder:values) {
          context.write(key, new TradeBuyOrdersOutputObject(buyorder));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradeBuyOrdersRow implements Writable  {
    int oid, cid, sid, qty, tid ;
    String status;
    BigDecimal bid;   
    Timestamp orderTime;
    
    public TradeBuyOrdersRow (){
    }
    
    public TradeBuyOrdersRow (int oid, int cid, int sid, int tid, int qty, String status, BigDecimal bid, Timestamp orderTime){
      this.oid=oid;
      this.cid=cid;
      this.sid=sid;
      this.tid=tid;
      this.qty=qty;
      this.status=status;
      this.bid=bid;
      this.orderTime=orderTime;
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

    public int getQty() {
      return qty;
    }

    public void setQty(int qty) {
      this.qty = qty;
    }

    public int getTid() {
      return tid;
    }

    public void setTid(int tid) {
      this.tid = tid;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }

    public BigDecimal getBid() {
      return bid;
    }

    public void setBid(BigDecimal bid) {
      this.bid = bid;
    }

    public Timestamp getOrderTime() {
      return orderTime;
    }

    public void setOrderTime(Timestamp orderTime) {
      this.orderTime = orderTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {   
      System.out.println("writing BuyOrders oid: " + oid + " cid: " + cid + " sid: " + sid + " qty: " + qty + " status: " + status + " bid: " + bid.toPlainString() + " orderTime: " + orderTime.getTime() + "tid: " +  tid);
      out.writeInt(oid);
      out.writeInt(cid);
      out.writeInt(sid);
      out.writeInt(tid);
      out.writeInt(qty);
      out.writeUTF(status);
      out.writeUTF(bid.toPlainString());      
      out.writeLong(orderTime.getTime());
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      oid=in.readInt();
      cid= in.readInt();
      sid = in.readInt();
      tid=in.readInt();
      qty=in.readInt();
      status=in.readUTF();
      bid=new BigDecimal(in.readUTF());
      orderTime=new Timestamp(in.readLong());
    }
  }

  public static class TradeBuyOrdersOutputObject  {
    int oid, cid, sid, qty, tid ;
    String status;
    BigDecimal bid;   
    Timestamp orderTime;
    
    public TradeBuyOrdersOutputObject (int oid, int cid, int sid, int qty, int tid, String status, BigDecimal bid, Timestamp orderTime){
      this.oid=oid;
      this.cid=cid;
      this.sid=sid;
      this.tid=tid;
      this.qty=qty;
      this.status=status;
      this.bid=bid;
      this.orderTime=orderTime;
    }       
    
    public TradeBuyOrdersOutputObject(TradeBuyOrdersRow row) {
      this.oid=row.oid;
      this.cid=row.cid;
      this.sid=row.sid;
      this.tid=row.tid;
      this.qty=row.qty;
      this.status=row.status;
      this.bid=row.bid;
      this.orderTime=row.orderTime;
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
    
    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }

    public void setQty(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,qty);
    }
    
    public void setStatus(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,status);
    }

    public void setBid(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,bid);
    }
    
    public void setOrderTime(int i, PreparedStatement ps) throws SQLException  {
      ps.setTimestamp(i,orderTime);
    }
    
  }

  public int run(String[] args) throws Exception {
    GfxdDataSerializable.initTypes();

    Configuration conf = getConf();
    
    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("TradeBuyOrdersHdfsDataVerifier.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    
    
    Job job = Job.getInstance(conf, "TradeBuyOrdersHdfsDataVerifierV2");
    job.setJobName("TradeBuyOrdersHdfsDataVerifierV2");
    job.setInputFormatClass(RowInputFormat.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    
      
    job.setMapperClass(HdfsDataMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(TradeBuyOrdersRow.class);   
    
    job.setReducerClass(HdfsDataReducer.class);  
    job.setOutputKeyClass(Key.class);
    job.setOutputValueClass(TradeBuyOrdersOutputObject.class);
    
    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());
    
    return job.waitForCompletion(false) ? 0 : 1;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradeBuyOrdersHdfsDataVerifier.main() invoked with " + args);    
    int rc = ToolRunner.run(new Configuration(),new TradeBuyOrdersHdfsDataVerifierV2(), args);
    System.exit(rc);
  }
}
