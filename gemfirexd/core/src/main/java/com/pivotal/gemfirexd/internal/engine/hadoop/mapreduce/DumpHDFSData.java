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
package com.pivotal.gemfirexd.internal.engine.hadoop.mapreduce;

import java.io.IOException;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HDFSRegionDirector.HdfsRegionManager;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.hadoop.mapreduce.Row;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat;
import com.pivotal.gemfirexd.hadoop.mapreduce.RowRecordReader;
import com.pivotal.gemfirexd.internal.engine.Misc;

/**
 * A utility for dumping all of the rows from
 * hoplogs in HDFS into a CSV file.
 * @author dsmith
 *
 */
public class DumpHDFSData {
  
  public static void main(String[] args) throws IOException, InterruptedException, SQLException {
    if(args.length < 3 || args.length > 4) {
      System.err.println("Usage: DumpHDFSData namenode_url homedir tablename");
      System.err.println("  Dumps the RAW data for the table tablename into a CSV format for debugging purposes");
      System.exit(1);
    }
    
    
    String namenodeURL = args[0];
    String homeDir = args[1];
    String table = args[2];
    
    
    Configuration conf = new Configuration();
    conf.set("fs.default.name", namenodeURL);
    FileSystem fs = FileSystem.get(conf);
    
    PrintStream out = new PrintStream(table + ".csv");
    try {
      String fullTable = RowInputFormat.getFullyQualifiedTableName(table);
      String folder = HdfsRegionManager.getRegionFolder(Misc.getRegionPath(fullTable));
      RemoteIterator<LocatedFileStatus> fileItr = fs.listFiles(new Path(homeDir + "/" + folder), true);
      
      conf.set(RowInputFormat.HOME_DIR, homeDir);
      conf.set(RowInputFormat.INPUT_TABLE, table);
      
      boolean wroteHeader = false;
      TaskAttemptContextImpl context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
      
      while(fileItr.hasNext()) {
        LocatedFileStatus file = fileItr.next();
        Path path = file.getPath();
        if(!path.getName().endsWith("hop")) {
          continue;
        }
        CombineFileSplit split = new CombineFileSplit(new Path[] { path } , new long[] { file.getLen()});
        RowRecordReader reader = new RowRecordReader();
        reader.initialize(split, context);
        while(reader.nextKeyValue()) {
          Row row = reader.getCurrentValue();
          ResultSet rs = row.getRowAsResultSet();
          Type op = row.getEventType();
          long ts = row.getTimestamp();

          int numColumns = rs.getMetaData().getColumnCount();
          if(!wroteHeader) {
            out.print("timestamp,operation,path");
            for(int i =1; i <= numColumns; i++) {
              out.print(",");
              out.print(rs.getMetaData().getColumnName(i));
            }
            out.println();
            wroteHeader = true;
          }

          out.print(ts);
          out.print(",");
          out.print(op);
          out.print(",");
          out.print(path);
          for(int i =1; i <= numColumns; i++) {
            out.print(",");
            String s= rs.getString(i);
            if(s != null) {
              s = s.replaceAll("([,\n])", "\\\\1");
            } else {
              s = "NULL";
            }
            out.print(s);
          }
          out.println();
        }
      }
    
    } finally {
      out.close();
    }
  }
}
