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
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

import com.gemstone.gemfire.cache.Operation;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.GFInputFormat;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.GFOutputFormat;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.mapreduce.GFKey;

import hydra.*;
import util.*;

public class KnownKeysMRv2 extends Configured implements Tool {

  public static class KnownKeysMRv2Mapper extends Mapper<GFKey, PersistedEventImpl, GFKey, PEIWritable> {

    // Identity mapper (log and write out processed key/value pairs, the value is the PersistedEventImpl)
    public void map(GFKey key, PersistedEventImpl value, Context context) throws IOException, InterruptedException {

      String keyStr = (String)key.getKey();
      Operation op = value.getOperation();
      ValueHolder entryValue = null;
      System.out.println("map method invoked with " + keyStr + " " + op.toString());
      try {
        entryValue = (ValueHolder)value.getDeserializedValue();
      } catch (ClassNotFoundException e) {
        System.out.println("KnownKeysMRv2.map() caught " + e + " : " + TestHelper.getStackTrace(e));
      }
      context.write(key, new PEIWritable(value));
    }
  }

  public static class KnownKeysMRv2Reducer extends Reducer<GFKey, PEIWritable, Object, Object> {
    public void reduce(GFKey key, Iterable<PEIWritable> values, Context context) throws IOException, InterruptedException {
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
      System.out.println("KnownKeysMRv2.reduce() invoked with " + keyStr);
      for (PEIWritable value:values) {
        PersistedEventImpl event = value.getEvent();
        Operation op = event.getOperation();

        ValueHolder vh = null;
        if (op.isDestroy()) {
          destroyed = true;
        } else {
          try {
            vh = (ValueHolder)event.getDeserializedValue();
          } catch (ClassNotFoundException e) {
            System.out.println("KnownKeysMRv2.map() caught " + e + " : " + TestHelper.getStackTrace(e));
          }
          if (op.isUpdate()) {
            updateValue = vh;
          } else {
            createValue = vh;
          }
        }
        System.out.println("KnownKeysMRv2.reduce() record: " + op.toString() + ": key = " + keyStr + " and op " + op.toString());
      }
      if (!destroyed) {
        if (updateValue != null) {
          context.write(key.getKey(), updateValue);
        } else {
          context.write(key.getKey(), createValue);
        }
      }
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    String locatorHost = args[0];
    int locatorPort = Integer.parseInt(args[1]);
    String hdfsHomeDir = args[2];

    System.out.println("KnownKeysMRv2 invoked with args (locatorHost = " + locatorHost + " locatorPort = " + locatorPort + " hdfsHomeDir = " + hdfsHomeDir);

    Configuration conf = getConf();
    conf.set(GFInputFormat.INPUT_REGION, "partitionedRegion");
    conf.set(GFInputFormat.HOME_DIR, hdfsHomeDir);
    conf.setBoolean(GFInputFormat.CHECKPOINT, false);
    conf.set(GFOutputFormat.REGION, "validationRegion");
    conf.set(GFOutputFormat.LOCATOR_HOST, locatorHost);
    conf.setInt(GFOutputFormat.LOCATOR_PORT, locatorPort);

    Job job = Job.getInstance(conf, "knownKeysMRv2");
    job.setInputFormatClass(GFInputFormat.class);
    job.setOutputFormatClass(GFOutputFormat.class);

    job.setMapperClass(KnownKeysMRv2Mapper.class);
    job.setMapOutputKeyClass(GFKey.class);
    job.setMapOutputValueClass(PEIWritable.class);

    job.setReducerClass(KnownKeysMRv2Reducer.class);
    //job.setOutputKeyClass(String.class);
    //job.setOutputValueClass(ValueHolder.class);

    return job.waitForCompletion(false) ? 0 : 1;

  }

  public static void main(String[] args) throws Exception {
    int rc = ToolRunner.run(new Configuration(), new KnownKeysMRv2(), args);
    System.exit(rc);
  }
}
