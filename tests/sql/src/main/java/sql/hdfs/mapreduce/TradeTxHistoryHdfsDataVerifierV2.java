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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;

import org.apache.hadoop.mapreduce.*;

import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.hadoop.mapreduce.*;

public class TradeTxHistoryHdfsDataVerifierV2 extends Configured implements Tool {

  public static class HdfsDataMapper extends  Mapper<Key, Row, Key, TradeTxHistoryRow> {
    
    @Override
    public void map(Key key, Row value, Context context) throws IOException , InterruptedException{           
      try {        
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet(); 
        String text="Key" + rs.getInt("cid") + rs.getInt("sid") + rs.getInt("tid") + rs.getInt("qty");
        Key k = new Key();
        k.setKey(CacheServerHelper.serialize("Key" + text));         
        context.write(k, new TradeTxHistoryRow(rs.getInt("oid"), rs.getInt("cid"), rs.getInt("sid"), rs.getInt("tid"), rs.getInt("qty"),  rs.getString("type"),  rs.getBigDecimal("price"), rs.getTimestamp("ordertime")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends  Reducer<Key, TradeTxHistoryRow, Key, TradeTxHistoryOutputObject> {
    @Override
    public void reduce(Key key, Iterable<TradeTxHistoryRow> values, Context context) throws IOException , InterruptedException {            
      try {
        for (TradeTxHistoryRow history : values) {
          context.write(key, new TradeTxHistoryOutputObject(history));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradeTxHistoryRow implements Writable  {    
    int oid, cid, sid, qty, tid ;
    String type;
    BigDecimal price;   
    Timestamp orderTime;
    
    public TradeTxHistoryRow (){
    }
    
    public TradeTxHistoryRow (int oid, int cid, int sid, int tid, int qty, String type, BigDecimal price, Timestamp orderTime){      
      this.oid=oid;
      this.cid=cid;
      this.sid=sid;
      this.tid=tid;
      this.qty=qty;
     this.type=type;
      this.price=price;
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

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public BigDecimal getPrice() {
      return price;
    }

    public void setPrice(BigDecimal price) {
      this.price = price;
    }

    public Timestamp getOrderTime() {
      return orderTime;
    }

    public void setOrderTime(Timestamp orderTime) {
      this.orderTime = orderTime;
    }

    @Override
    public void write(DataOutput out) throws IOException {    
      if (orderTime == null)  orderTime = new Timestamp(1000);
      if (type == null || type.equals(""))  type = " ";
      System.out.println("writing Txhistory oid: " + oid +" cid: " + cid + " sid: " + sid + " tid: " + tid + " qty: " + qty + " type: " + type + " price: " + price.toPlainString() + " orderTime: " + orderTime);
      out.writeInt(oid);
      out.writeInt(cid);
      out.writeInt(sid);
      out.writeInt(tid);
      out.writeInt(qty);
      out.writeUTF(type);
      out.writeUTF(price.toPlainString());      
      out.writeLong(orderTime.getTime());
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      oid=in.readInt();
      cid= in.readInt();
      sid = in.readInt();
      tid=in.readInt();
      qty=in.readInt();
      type=in.readUTF();
      price=new BigDecimal(in.readUTF());
      orderTime=new Timestamp(in.readLong());
      type=type.equals(" ")?null:type;
      orderTime=orderTime.equals(new Timestamp(1000))?null:orderTime;
    }
  }

  public static class TradeTxHistoryOutputObject  {
    int oid, cid, sid, qty, tid ;
    String type;
    BigDecimal price;   
    Timestamp orderTime;
    
    public TradeTxHistoryOutputObject (int oid, int cid, int sid, int qty, int tid, String type, BigDecimal price, Timestamp orderTime){
      Timestamp ts = new Timestamp (1000);
      this.oid=oid;
      this.cid=cid;
      this.sid=sid;
      this.tid=tid;
      this.qty=qty;
      this.type=type;
      this.price=price;
      this.orderTime=orderTime;
    }       
    
    public TradeTxHistoryOutputObject(TradeTxHistoryRow row) {
      this.oid=row.oid;
      this.cid=row.cid;
      this.sid=row.sid;
      this.tid=row.tid;
      this.qty=row.qty;
      this.type=row.type;
      this.price=row.price;
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
    
    public void setType(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,type);
    }

    public void setPrice(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,price);
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

    System.out.println("TradeTxHistoryHdfsDataVerifierV2.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    
    
    Job job = Job.getInstance(conf, "TradeTxHistoryHdfsDataVerifierV2");
    job.setJobName("TradeTxHistoryHdfsDataVerifierV2");
    job.setInputFormatClass(RowInputFormat.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    
      
    job.setMapperClass(HdfsDataMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(TradeTxHistoryRow.class);   
    
    job.setReducerClass(HdfsDataReducer.class);  
    job.setOutputKeyClass(Key.class);
    job.setOutputValueClass(TradeTxHistoryOutputObject.class);
    
    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());
    
    return job.waitForCompletion(false) ? 0 : 1;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradeTxHistoryHdfsDataVerifierV2.main() invoked with " + args);    
    int rc = ToolRunner.run(new Configuration(),new TradeTxHistoryHdfsDataVerifierV2(), args);
    System.exit(rc);
  }
}
