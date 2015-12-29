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

public class TradeNetworthHdfsDataVerifier extends Configured implements Tool {

  public static class HdfsDataMapper extends MapReduceBase implements Mapper<Key, Row, Text, TradeNetworthRow> {
    
    @Override
    public void map(Key key, Row value, OutputCollector<Text, TradeNetworthRow> output, Reporter reporter) throws IOException {           
      try {        
        if ( ! value.getEventType().equals(Type.AFTER_DELETE)) {
        ResultSet rs = value.getRowAsResultSet();
        int cid=rs.getInt("cid");
        output.collect(new Text(Integer.toString(cid)), new TradeNetworthRow(cid, rs.getInt("tid"), rs.getBigDecimal("cash"), rs.getBigDecimal("securities"), rs.getInt("loanlimit"), rs.getBigDecimal("availloan")) );
        }
      } catch (SQLException se) {
        System.err.println("mapper -  -Error logging result set" + se);
        throw new  IOException(se);
      }
    }
  }

  public static class HdfsDataReducer extends MapReduceBase implements Reducer<Text, TradeNetworthRow, Key, TradeNetworthOutputObject> {
    @Override
    public void reduce(Text key, Iterator<TradeNetworthRow> values, OutputCollector<Key, TradeNetworthOutputObject> output, Reporter reporter) throws IOException {            
      try {
        while (values.hasNext()) {
          TradeNetworthRow networth = values.next();
          Key k = new Key();
          k.setKey(CacheServerHelper.serialize(networth.getCid()));          
          output.collect(k, new TradeNetworthOutputObject(networth));
        }
      } catch (Exception e) {
        System.out.println("error in reducer " + e.getMessage());
        throw new IOException(e);
      }
  }
  }
  public static class TradeNetworthRow implements Writable  { 
    int cid, tid, loanLimit;
    BigDecimal cash, securities,availLoan;   
    
    
    public TradeNetworthRow (){
    }
    
    public TradeNetworthRow (int cid, int tid, BigDecimal cash, BigDecimal securities, int loanLimit, BigDecimal availLoan){
      this.cid=cid;
      this.tid=tid;
      this.cash=cash;
      this.securities=securities;
      this.loanLimit=loanLimit;
      this.availLoan=availLoan;
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

    public BigDecimal getCash() {
      return cash;
    }

    public void setCash(BigDecimal cash) {
      this.cash = cash;
    }

    public BigDecimal getSecurities() {
      return securities;
    }

    public void setSecurities(BigDecimal securities) {
      this.securities = securities;
    }

    public int getLoanLimit() {
      return loanLimit;
    }

    public void setLoanLimit(int loanLimit) {
      this.loanLimit = loanLimit;
    }

    public BigDecimal getAvailLoan() {
      return availLoan;
    }

    public void setAvailLoan(BigDecimal availLoan) {
      this.availLoan = availLoan;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      System.out.println("writing Networth cid: " + cid + " tid: " + tid + " cash: " + cash.toPlainString() + " securities: " + securities.toPlainString() + " loanLimit: " + loanLimit + " availLoan: " + availLoan.toPlainString());
      out.writeInt(cid);
      out.writeInt(tid);
      out.writeUTF(cash.toPlainString());
      out.writeUTF(securities.toPlainString());
      out.writeInt(loanLimit);
      out.writeUTF(availLoan.toPlainString());
      
    }
  
    @Override
    public void readFields(DataInput in) throws IOException {
      cid = in.readInt();
      tid=in.readInt();
      cash=new BigDecimal(in.readUTF());
      securities=new BigDecimal(in.readUTF());
      loanLimit=in.readInt();
      availLoan=new BigDecimal(in.readUTF());
    }
  }

  public static class TradeNetworthOutputObject  {
    int cid, tid , loanLimit;
    BigDecimal cash, securities,availLoan;   
    
    
    public TradeNetworthOutputObject (){
    }
    
    public TradeNetworthOutputObject (int cid, int tid, BigDecimal cash, BigDecimal securities, int loanLimit, BigDecimal availLoan){
      this.cid=cid;
      this.tid=tid;
      this.cash=cash;
      this.securities=securities;
      this.loanLimit=loanLimit;
      this.availLoan=availLoan;
    }
    
    public TradeNetworthOutputObject (TradeNetworthRow row){
      this.cid=row.cid;
      this.tid=row.tid;
      this.cash=row.cash;
      this.securities=row.securities;
      this.loanLimit=row.loanLimit;
      this.availLoan=row.availLoan;
    }
      
    public void setCid(int i, PreparedStatement ps) throws SQLException {
      ps.setInt(i,cid);
    }


    public void setTid(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,tid);
    }
   

    public void setCash(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,cash);
    }

    public void setSecurities(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,securities);
    }  
    
    public void setLoanLimit(int i, PreparedStatement ps) throws SQLException  {
      ps.setInt(i,loanLimit);
    }  
    
    public void setAvailLoan(int i, PreparedStatement ps) throws SQLException  {
      ps.setBigDecimal(i,availLoan);
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
    conf.setMapOutputValueClass(TradeNetworthRow.class);
    
    conf.setReducerClass(HdfsDataReducer.class);
    conf.set(RowOutputFormat.OUTPUT_TABLE, tableName +"_HDFS");
    //conf.set(GfxdOutputFormat.OUTPUT_SCHEMA, "APP");
    conf.set(RowOutputFormat.OUTPUT_URL, url);
    conf.setOutputFormat(RowOutputFormat.class);
    conf.setOutputKeyClass(Key.class);
    conf.setOutputValueClass(TradeNetworthOutputObject.class);

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
    int rc = ToolRunner.run(new TradeNetworthHdfsDataVerifier(), args);
    System.exit(rc);
  }
}
