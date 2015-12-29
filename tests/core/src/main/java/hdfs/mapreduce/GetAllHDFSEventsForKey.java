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
package hdfs.mapreduce;

import java.io.IOException;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.util.*;

import com.gemstone.gemfire.cache.Operation;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapred.GFInputFormat;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapred.GFOutputFormat;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.GFKey;

import hydra.*;
import util.*;

import hdfs.HDFSUtil;

public class GetAllHDFSEventsForKey extends Configured implements Tool {

  public static class GetAllHDFSEventsForKeyMapper extends MapReduceBase implements Mapper<GFKey, PersistedEventImpl, GFKey, PEIWritable> {

    private String searchKeys = null;
    public void configure(JobConf job) {
      searchKeys = job.get("getAllHDFSEventsForKey.searchKeys");
    }      

    // Identity mapper (log and write out processed key/value pairs, the value is the PersistedEventImpl)
    public void map(GFKey key, PersistedEventImpl value, OutputCollector<GFKey, PEIWritable> output, Reporter reporter) throws IOException {

      // create a list of problemKeys (form searchKey String)
      List problemKeys = new ArrayList();
      StringTokenizer tokenizer = new StringTokenizer(searchKeys);
      while (tokenizer.hasMoreTokens()) {
        problemKeys.add(tokenizer.nextToken());
      }
      StringBuffer aStr = new StringBuffer();
      for (Iterator it = problemKeys.iterator(); it.hasNext();) {
        aStr.append(it.next() + " ");
      } 
      System.out.println("GetAllHDFSEventsForKeyMapper.problemKeys = " + aStr.toString());

      String keyStr = (String)key.getKey();
      Operation op = value.getOperation();
      ValueHolder entryValue = null;
      if (problemKeys.contains(keyStr)) {
         System.out.println("map method invoked with " + keyStr + " " + op.toString());
      }
      try {
        entryValue = (ValueHolder)value.getDeserializedValue();
      } catch (ClassNotFoundException e) {
        System.out.println("GetAllHDFSEventsForKey.map() caught " + e + " : " + TestHelper.getStackTrace(e));
      }
      if (problemKeys.contains(keyStr)) {
        output.collect(key, new PEIWritable(value));
      }
    }
  }

  public static class GetAllHDFSEventsForKeyReducer extends MapReduceBase implements Reducer<GFKey, PEIWritable, Object, Object> {
    public void reduce(GFKey key, Iterator<PEIWritable> values, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
      String keyStr = (String)key.getKey();
      System.out.println("GetAllHDFSEventsForKey.reduce() invoked with " + keyStr);
      int eventCounter = 0;
      while (values.hasNext()) {
        eventCounter++;
        PEIWritable peiWritable = (PEIWritable)values.next();
        PersistedEventImpl event = peiWritable.getEvent();
        Operation op = event.getOperation();

        StringBuffer newKey = new StringBuffer();
        newKey.append(keyStr + "_");
        newKey.append(eventCounter + "+");
        newKey.append(op.toString());

        System.out.println("GetAllHDFSEventsForKey.reduce() record: " + op.toString() + ": key = " + keyStr + " and op " + op.toString());

        Object o = null;
        if (!op.isDestroy()) {
          try {
            o = event.getDeserializedValue();
          } catch (ClassNotFoundException e) {
            System.out.println("GetAllHDFSEventsForKey.reduce() caught " + e + " : " + TestHelper.getStackTrace(e));
          }
        } else {
          o = "DESTROYED";
        }
        output.collect(newKey.toString(), o);
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    String locatorHost = args[0];
    int locatorPort = Integer.parseInt(args[1]);
    String hdfsHomeDir = args[2];

    StringBuffer searchKeys = new StringBuffer();
    for (int i = 3; i < args.length; i++) {
      searchKeys.append(args[i] + " ");
    }

    System.out.println("GetAllHDFSEventsForKey invoked with args (locatorHost = " + locatorHost + " locatorPort = " + locatorPort + " hdfsHomeDir = " + hdfsHomeDir + " searchKeys = " + searchKeys);

    Configuration conf = getConf();
    JobConf jobConf =  new JobConf(conf, GetAllHDFSEventsForKey.class);
    jobConf.setJobName("getAllHDFSEventsForKey");

    jobConf.set("getAllHDFSEventsForKey.searchKeys", searchKeys.toString());

    jobConf.set(GFInputFormat.INPUT_REGION, "partitionedRegion");
    jobConf.set(GFInputFormat.HOME_DIR, hdfsHomeDir);
    jobConf.setBoolean(GFInputFormat.CHECKPOINT, false);

    jobConf.set(GFOutputFormat.REGION, HDFSUtil.HDFS_RESULT_REGION); 
    jobConf.set(GFOutputFormat.LOCATOR_HOST, locatorHost); 
    // User can also configure server host if needed
    // conf.set(GFOutputFormat.SERVER_HOST, "localhost");

    jobConf.setInt(GFOutputFormat.LOCATOR_PORT, locatorPort);
    // User can also configure server port if needed
    // conf.setInt(GFOutputFormat.SERVER_PORT, 40404);

    jobConf.setMapperClass(GetAllHDFSEventsForKeyMapper.class);
    jobConf.setInputFormat(GFInputFormat.class);
    jobConf.setMapOutputKeyClass(GFKey.class);
    jobConf.setMapOutputValueClass(PEIWritable.class);

    jobConf.setReducerClass(GetAllHDFSEventsForKeyReducer.class);
    jobConf.setOutputFormat(GFOutputFormat.class);

    JobClient.runJob(jobConf);
    return 0;

  }

  public static void main(String[] args) throws Exception {
    System.out.println("GetAllHDFSEventsForKey.main() invoked with " + args);
    int rc = ToolRunner.run(new Configuration(), new GetAllHDFSEventsForKey(), args);
    System.exit(rc);
  }
}
