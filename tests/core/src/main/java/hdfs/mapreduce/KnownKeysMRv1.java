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

public class KnownKeysMRv1 extends Configured implements Tool {

  public static class KnownKeysMRv1Mapper extends MapReduceBase implements Mapper<GFKey, PersistedEventImpl, GFKey, PEIWritable> {

    // Identity mapper (log and write out processed key/value pairs, the value is the PersistedEventImpl)
    public void map(GFKey key, PersistedEventImpl value, OutputCollector<GFKey, PEIWritable> output, Reporter reporter) throws IOException {

      String keyStr = (String)key.getKey();
      Operation op = value.getOperation();
      ValueHolder entryValue = null;
      System.out.println("map method invoked with " + keyStr + " " + op.toString());
      try {
        entryValue = (ValueHolder)value.getDeserializedValue();
      } catch (ClassNotFoundException e) {
        System.out.println("KnownKeysMRv1.map() caught " + e + " : " + TestHelper.getStackTrace(e));
      }
      output.collect(key, new PEIWritable(value));
    }
  }

  public static class KnownKeysMRv1Reducer extends MapReduceBase implements Reducer<GFKey, PEIWritable, Object, Object> {
    public void reduce(GFKey key, Iterator<PEIWritable> values, OutputCollector<Object, Object> output, Reporter reporter) throws IOException {
      // For a particular key ... process all records and output what we would have expected in this concKnownKeys test
      // Note that we either
      // 1. do a single create
      // 2. create + update
      // 3. create + destroy
      // look at all ops ... and output either
      // 1. create
      // 2. create (with value from update)
      // 3. do nothing (overall result is destroy, so do not create the entry in the gemfire validation region
      String keyStr = (String)key.getKey();
      ValueHolder updateValue = null;
      ValueHolder createValue = null;
      boolean destroyed = false;
      System.out.println("KnownKeysMRv1.reduce() invoked with " + keyStr);
      while (values.hasNext()) {
        PEIWritable peiWritable = (PEIWritable)values.next();
        PersistedEventImpl event = peiWritable.getEvent();
        Operation op = event.getOperation();

        ValueHolder vh = null;
        if (op.isDestroy()) {
          destroyed = true;
        } else {
          try {
            vh = (ValueHolder)event.getDeserializedValue();
          } catch (ClassNotFoundException e) {
            System.out.println("KnownKeysMRv1.map() caught " + e + " : " + TestHelper.getStackTrace(e));
          }
          if (op.isUpdate()) {
            updateValue = vh;
          } else {
            createValue = vh;
          }
        }
        System.out.println("KnownKeysMRv1.reduce() record: " + op.toString() + ": key = " + keyStr + " and op " + op.toString());
      }
      if (!destroyed) {
        if (updateValue != null) {
          output.collect(keyStr, updateValue);
        } else {
          output.collect(keyStr, createValue);
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    String locatorHost = args[0];
    int locatorPort = Integer.parseInt(args[1]);
    String hdfsHomeDir = args[2];

    System.out.println("KnownKeysMRv1 invoked with args (locatorHost = " + locatorHost + " locatorPort = " + locatorPort + " hdfsHomeDir = " + hdfsHomeDir);

    Configuration conf = getConf();
    JobConf jobConf =  new JobConf(conf, KnownKeysMRv1.class);
    jobConf.setJobName("knownKeysMRv1");

    jobConf.set(GFInputFormat.INPUT_REGION, "partitionedRegion");
    jobConf.set(GFInputFormat.HOME_DIR, hdfsHomeDir);
    jobConf.setBoolean(GFInputFormat.CHECKPOINT, false);

    jobConf.set(GFOutputFormat.REGION, "validationRegion");
    jobConf.set(GFOutputFormat.LOCATOR_HOST, locatorHost); 
    // User can also configure server host if needed
    // conf.set(GFOutputFormat.SERVER_HOST, "localhost");

    jobConf.setInt(GFOutputFormat.LOCATOR_PORT, locatorPort);
    // User can also configure server port if needed
    // conf.setInt(GFOutputFormat.SERVER_PORT, 40404);

    jobConf.setMapperClass(KnownKeysMRv1Mapper.class);
    jobConf.setInputFormat(GFInputFormat.class);
    jobConf.setMapOutputKeyClass(GFKey.class);
    jobConf.setMapOutputValueClass(PEIWritable.class);

    jobConf.setReducerClass(KnownKeysMRv1Reducer.class);
    jobConf.setOutputFormat(GFOutputFormat.class);
    //jobConf.setOutputKeyClass(GFKey.class);
    //jobConf.setOutputValueClass(ValueHolder.class);

    JobClient.runJob(jobConf);
    return 0;

  }

  public static void main(String[] args) throws Exception {
    System.out.println("KnownKeysMRv1.main() invoked with " + args);
    int rc = ToolRunner.run(new Configuration(), new KnownKeysMRv1(), args);
    System.exit(rc);
  }
}
