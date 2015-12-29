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
package demo.gfxd.mr1;

import com.pivotal.gemfirexd.hadoop.mapred.RowInputFormat;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
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

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

public class BusyAirports extends Configured implements Tool {

  public static class SampleMapper extends MapReduceBase
      implements Mapper<Object, ResultSet, Text, IntWritable> {

    private final static IntWritable countOne = new IntWritable(1);
    private final IntWritable counter = new IntWritable();
    private final Text reusableText = new Text();

    @Override
    public void map(Object key, ResultSet rs,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {

      String origAirport;
      String destAirport;

      try {
        while (rs.next()) {
          origAirport = rs.getString("ORIG_AIRPORT");
          destAirport = rs.getString("DEST_AIRPORT");
          reusableText.set(origAirport);
          output.collect(reusableText, countOne);
          reusableText.set(destAirport);
          output.collect(reusableText, countOne);
        }
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  public static class SampleReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text token, Iterator<IntWritable> values,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int sum = 0;

      while (values.hasNext()) {
        sum += values.next().get();
      }

      output.collect(token, new IntWritable(sum));
    }
  }

  public int run(String[] args) throws Exception {

    GfxdDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("Busy Airport Count");

    Path outputPath = new Path(args[0]);
    String hdfsHomeDir = args[1];
    String tableName = args[2];

    outputPath.getFileSystem(conf).delete(outputPath, true);

    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);

    conf.setInputFormat(RowInputFormat.class);
    conf.setMapperClass(SampleMapper.class);
    conf.setMapOutputKeyClass(Text.class);
    conf.setMapOutputValueClass(IntWritable.class);

    conf.setReducerClass(SampleReducer.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);

    FileOutputFormat.setOutputPath(conf, outputPath);

    JobClient.runJob(conf);
    return 0;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("SampleApp.main() invoked with " + args);
    int rc = ToolRunner.run(new BusyAirports(), args);
    System.exit(rc);
  }
}

