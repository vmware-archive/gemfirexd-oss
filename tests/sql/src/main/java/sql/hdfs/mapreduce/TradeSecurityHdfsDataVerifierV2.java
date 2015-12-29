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

public class TradeSecurityHdfsDataVerifierV2 extends Configured implements Tool {

  public static class HdfsDataMapper extends Mapper<Key, Row, Key, TradeSecurityRow> {   
    public void map(Key key, Row value, Context context) throws IOException , InterruptedException {           
      try {        
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();  
        int secId=rs.getInt("sec_id");
        Key k = new Key();
        k.setKey(CacheServerHelper.serialize(secId));
        context.write(k, new TradeSecurityRow(secId, rs.getInt("tid"), rs.getString("symbol"), rs.getString("exchange"), rs.getBigDecimal("price")) );
        }
      } catch (SQLException se) {
        System.out.println("error in mapper " + se.getMessage());
        throw new  IOException(se);
      }
      
    }
  }
    

  public static class HdfsDataReducer extends Reducer<Key, TradeSecurityRow, Key, TradeSecurityOutputObject > {    
    @Override
    public void reduce(Key key, Iterable<TradeSecurityRow> values, Context context) throws IOException , InterruptedException  {    
      try {        
        for (TradeSecurityRow trade : values ) {       
          context.write(key, new TradeSecurityOutputObject(trade));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradeSecurityRow implements Writable  {
    int secId, tid;
    String symbol, exchange;
    BigDecimal price;   
    
    
    public TradeSecurityRow (){
    }
    
    public TradeSecurityRow (int secId, int tid, String symbol, String exchange , BigDecimal price){
      this.secId=secId;
      this.tid=tid;
      this.symbol=symbol;
      this.exchange=exchange;
      this.price=price;      
    }
         
    
    public int getSecId() {
      return secId;
    }

    public void setSecId(int secId) {
      this.secId = secId;
    }

    public int getTid() {
      return tid;
    }

    public void setTid(int tid) {
      this.tid = tid;
    }

    public String getSymbol() {
      return symbol;
    }

    public void setSymbol(String symbol) {
      this.symbol = symbol;
    }

    public String getExchange() {
      return exchange;
    }

    public void setExchange(String exchange) {
      this.exchange = exchange;
    }

    public BigDecimal getPrice() {
      return price;
    }

    public void setPrice(BigDecimal price) {
      this.price = price;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      System.out.println("writing Securities sid: " + secId + " price: " + price.toPlainString() + " symbol: " + symbol + " exchange: " + exchange + " tid: " +  tid);
      out.writeInt(secId);
      out.writeInt(tid);
      out.writeUTF(price.toPlainString());   
      out.writeUTF(symbol);
      out.writeUTF(exchange);
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      secId = in.readInt();
      tid=in.readInt();     
      price = new BigDecimal (in.readUTF());      
      symbol=in.readUTF();
      exchange=in.readUTF();    
      System.out.println("reading Securities sid: " + secId + " price: " + price.toPlainString() + " symbol: " + symbol + " exchange: " + exchange + " tid: " +  tid);
    }
  }

  public static class TradeSecurityOutputObject  {
    int sec_id, tid;
    String symbol, exchange;
    BigDecimal price;   

    public TradeSecurityOutputObject(int secId, int tid, String symbol, String exchange, BigDecimal price) {
      this.sec_id=secId;
      this.tid=tid;
      this.symbol=symbol;
      this.exchange=exchange;
      this.price=price; 
    }
    
    public TradeSecurityOutputObject(TradeSecurityRow row) {
      System.out.println("in output object constructor");
      this.sec_id=row.secId;
      this.tid=row.tid;
      this.symbol=row.symbol;
      this.exchange=row.exchange;
      this.price=row.price; 
    }
    
    public void setSec_id(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,sec_id);
    }


    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }


    public void setSymbol(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,symbol);
    }


    public void setExchange(int i, PreparedStatement ps) throws SQLException  {
      ps.setString(i,exchange);
    }


    public void setPrice(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,price);
    }

    @Override
    public String toString() {     
      return "sec_id " + sec_id + " tid "  + tid + " exchange " + exchange;
    }      
    
    
    
  }

  public int run(String[] args) throws Exception {

    GfxdDataSerializable.initTypes();

    Configuration conf = getConf();
    
    String hdfsHomeDir = args[0];
    String url         = args[1];
    String tableName   = args[2];

    System.out.println("TradeSecurityHdfsDataVerifier.run() invoked with " 
                       + " hdfsHomeDir = " + hdfsHomeDir 
                       + " url = " + url
                       + " tableName = " + tableName);

    // Job-specific params
    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    
    
    Job job = Job.getInstance(conf, "TradeSecurityHdfsDataVerifierV2");
    job.setJobName("TradeSecurityHdfsDataVerifierV2");
    job.setInputFormatClass(RowInputFormat.class);
    job.setOutputFormatClass(RowOutputFormat.class);
    
      
    job.setMapperClass(HdfsDataMapper.class);
    job.setMapOutputKeyClass(Key.class);
    job.setMapOutputValueClass(TradeSecurityRow.class);   
    
    job.setReducerClass(HdfsDataReducer.class);  
    job.setOutputKeyClass(Key.class);
    job.setOutputValueClass(TradeSecurityOutputObject.class);
    
    StringBuffer aStr = new StringBuffer();
    aStr.append("HOME_DIR = " + conf.get(RowInputFormat.HOME_DIR) + " ");
    aStr.append("INPUT_TABLE = " + conf.get(RowInputFormat.INPUT_TABLE) + " ");
    aStr.append("OUTPUT_TABLE = " + conf.get(RowOutputFormat.OUTPUT_TABLE) + " ");
    aStr.append("OUTPUT_URL = " + conf.get(RowOutputFormat.OUTPUT_URL) + " ");
    System.out.println("VerifyHdfsData running with the following conf: " + aStr.toString());
    
    return job.waitForCompletion(false) ? 0 : 1;
  }
    
  public static void main(String[] args) throws Exception {
    System.out.println("TradeSecurityHdfsDataVerifier.main() invoked with " + args);    
    int rc = ToolRunner.run(new Configuration(),new TradeSecurityHdfsDataVerifierV2(), args);
    System.exit(rc);
  }
}
