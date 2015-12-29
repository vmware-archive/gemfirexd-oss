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
import com.pivotal.gemfirexd.hadoop.mapred.Key;
import com.pivotal.gemfirexd.hadoop.mapred.Row;
import com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import demo.gfxd.model.BusyAirportModel;
import demo.gfxd.utils.StringIntPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.net.URI;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

/**
 * This class builds on the functionality of BusyAirports by finding the most
 * busy airport. In this example this is achieved using a second MR job.
 * Essentially the output of the first job (BusyAirports) is sent to the second
 * job.
 *
 * The mapper of the 2nd job produces values of StringIntPairs which are simply
 * a tuple of the airport code and number of flights into/out of that airport.
 * By configuring a single reducer all of these tuples are sent to a single
 * task allowing the top airport to be determined.
 *
 * I'm sure there are better ways to approach this...
 */
public class TopBusyAirportGemfirexd extends Configured implements Tool {

  /**
   * Mapper used for first job. Produces tuples of the form:
   *
   *    MIA 1
   *    JFK 1
   *
   * This job is configured with a standard IntSumReducer to produce totals
   * for each airport code.
   */
  public static class SampleMapper extends MapReduceBase
      implements Mapper<Object, Row, Text, IntWritable> {

    private final static IntWritable countOne = new IntWritable(1);
    private final Text reusableText = new Text();

    @Override
    public void map(Object key, Row row,
        OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {

      String origAirport;
      String destAirport;

      try {
        ResultSet rs = row.getRowAsResultSet();
        origAirport = rs.getString("ORIG_AIRPORT");
        destAirport = rs.getString("DEST_AIRPORT");
        reusableText.set(origAirport);
        output.collect(reusableText, countOne);
        reusableText.set(destAirport);
        output.collect(reusableText, countOne);
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

  /**
   * Mapper and Reducer used for second job. Here we produce tuples of the
   * form:
   * <p/>
   * 1 [MIA 145]
   * 1 [JFK 231]
   */
  public static class TopBusyAirportMapper extends MapReduceBase
      implements Mapper<LongWritable, Text, Text, StringIntPair> {

    private static final Text textOne = new Text("1");

    @Override
    public void map(LongWritable key, Text value,
        OutputCollector<Text, StringIntPair> output, Reporter reporter)
        throws IOException {
      String[] splits = value.toString().split("\\s+");
      output.collect(textOne, new StringIntPair(splits[0], Integer.parseInt(splits[1])));
    }
  }

  /**
   * The single reducer receives all of the values and is able to determine
   * the top airport.
   */
  public static class TopBusyAirportReducer extends MapReduceBase
      implements Reducer<Text, StringIntPair, Key, BusyAirportModel> {

    @Override
    public void reduce(Text token, Iterator<StringIntPair> values,
        OutputCollector<Key, BusyAirportModel> output, Reporter reporter)
        throws IOException {
      String topAirport = null;
      int max = 0;

      while (values.hasNext()) {
        StringIntPair v = values.next();
        if (v.getSecond() > max) {
          max = v.getSecond();
          topAirport = v.getFirst();
        }
      }
      BusyAirportModel busy = new BusyAirportModel(topAirport, max);
      output.collect(new Key(), busy);
    }
  }

  public int run(String[] args) throws Exception {

    GfxdDataSerializable.initTypes();

    JobConf conf = new JobConf(getConf());
    conf.setJobName("Busy Airport Count");

    Path outputPath = new Path(args[0]);
    Path intermediateOutputPath = new Path(args[0] + "_int");
    String hdfsHomeDir = args[1];
    String tableName = args[2];

    outputPath.getFileSystem(conf).delete(outputPath, true);
    intermediateOutputPath.getFileSystem(conf).delete(intermediateOutputPath, true);

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

    FileOutputFormat.setOutputPath(conf, intermediateOutputPath);

    int rc = JobClient.runJob(conf).isSuccessful() ? 0 : 1;
    if (rc == 0) {
      JobConf topConf = new JobConf(getConf());
      topConf.setJobName("Top Busy Airport");

      String hdfsFS = topConf.get("fs.defaultFS");
      URI hdfsUri = URI.create(hdfsFS);
      hdfsUri.getHost();

      // Assume that SqlFire locator is running alongside the namenode
      topConf.set(RowOutputFormat.OUTPUT_URL, "jdbc:gemfirexd://" + hdfsUri.getHost() + ":1527");
      //topConf.set(ddGfxdOutputFormat.OUTPUT_SCHEMA, "APP");
      //topConf.set(GfxdOutputFormat.OUTPUT_TABLE, "BUSY_AIRPORT");
      topConf.set(RowOutputFormat.OUTPUT_TABLE, "APP.BUSY_AIRPORT");

      // Only run a single reducer
      topConf.setNumReduceTasks(1);

      FileInputFormat.setInputPaths(topConf, intermediateOutputPath);

      topConf.setInputFormat(TextInputFormat.class);
      topConf.setMapperClass(TopBusyAirportMapper.class);
      topConf.setMapOutputKeyClass(Text.class);
      topConf.setMapOutputValueClass(StringIntPair.class);

      topConf.setReducerClass(TopBusyAirportReducer.class);
      topConf.setOutputKeyClass(Key.class);
      topConf.setOutputValueClass(BusyAirportModel.class);
      topConf.setOutputFormat(RowOutputFormat.class);

      rc = JobClient.runJob(topConf).isSuccessful() ? 0 : 1;
    }
    return rc;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("SampleApp.main() invoked with " + args);
    int rc = ToolRunner.run(new TopBusyAirportGemfirexd(), args);
    System.exit(rc);
  }
}

