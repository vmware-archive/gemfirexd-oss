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

public class TradeSecurityHdfsDataVerifier extends Configured implements Tool {

  public static class HdfsDataMapper extends MapReduceBase implements Mapper<Key, Row, Text, TradeSecurityRow> {
    
    @Override
    public void map(Key key, Row value, OutputCollector<Text, TradeSecurityRow> output, Reporter reporter) throws IOException {           
      try {        
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();  
        int secId=rs.getInt("sec_id");
        output.collect(new Text(Integer.toString(secId)), new TradeSecurityRow(secId, rs.getInt("tid"), rs.getString("symbol"), rs.getString("exchange"), rs.getBigDecimal("price")) );
        }
      } catch (SQLException se) {
        throw new  IOException(se);
      }
      
    }
  }

  public static class HdfsDataReducer extends MapReduceBase implements Reducer<Text, TradeSecurityRow, Key, TradeSecurityOutputObject> {
    @Override
    public void reduce(Text key, Iterator<TradeSecurityRow> values, OutputCollector<Key, TradeSecurityOutputObject> output, Reporter reporter) throws IOException {            
      try {        
        while (values.hasNext()) {
          TradeSecurityRow trade = values.next();
          Key k = new Key();
          k.setKey(CacheServerHelper.serialize(trade.getSecId()));
          output.collect(k, new TradeSecurityOutputObject(trade));
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
      System.out.println("writing Securities sid: " + secId + " price: " + price.toPlainString() + " symbol: " + symbol + " exchange: " + exchange + "tid: " +  tid);
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
    
  }

  public int run(String[] args) throws Exception {

    GfxdDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("TradeSecurityHdfsDataVerifier");

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
    
    conf.setInputFormat(RowInputFormat.class);
    conf.setMapperClass(HdfsDataMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(TradeSecurityRow.class);
    
    conf.setReducerClass(HdfsDataReducer.class);
    conf.set(RowOutputFormat.OUTPUT_TABLE,tableName + "_HDFS");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    conf.setOutputFormat(RowOutputFormat.class);
    conf.setOutputKeyClass(Key.class);
    conf.setOutputValueClass(TradeSecurityOutputObject.class);

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
    System.out.println("TradeSecurityHdfsDataVerifier.main() invoked with " + args);    
    int rc = ToolRunner.run(new TradeSecurityHdfsDataVerifier(), args);
    System.exit(rc);
  }
}
