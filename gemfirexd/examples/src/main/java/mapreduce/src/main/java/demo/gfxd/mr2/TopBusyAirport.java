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
package demo.gfxd.mr2;

import com.pivotal.gemfirexd.hadoop.mapreduce.RowInputFormat;
import com.pivotal.gemfirexd.hadoop.mapreduce.Row;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import demo.gfxd.utils.StringIntPair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

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
public class TopBusyAirport extends Configured implements Tool {

  /**
   * Mapper used for first job. Produces tuples of the form:
   *
   *    MIA 1
   *    JFK 1
   *
   * This job is configured with a standard IntSumReducer to produce totals
   * for each airport code.
   */
  public static class SampleMapper extends Mapper<Object, Row, Text, IntWritable> {

    private final static IntWritable countOne = new IntWritable(1);
    private final Text reusableText = new Text();

    @Override
    public void map(Object key, Row row, Context context)
        throws IOException, InterruptedException {

      String origAirport;
      String destAirport;

      try {
        ResultSet rs = row.getRowAsResultSet();
        origAirport = rs.getString("ORIG_AIRPORT");
        destAirport = rs.getString("DEST_AIRPORT");
        reusableText.set(origAirport);
        context.write(reusableText, countOne);
        reusableText.set(destAirport);
        context.write(reusableText, countOne);
      } catch (SQLException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Mapper and Reducer used for second job. Here we produce tuples of the
   * form:
   *
   *   1 [MIA 145]
   *   1 [JFK 231]
   */
  public static class TopBusyAirportMapper extends Mapper<LongWritable, Text, Text, StringIntPair> {

    private static final Text textOne = new Text("1");

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String[] splits = value.toString().split("\\s+");
      context.write(textOne, new StringIntPair(splits[0], Integer.parseInt(splits[1])));
    }
  }

  /**
   * The single reducer receives all of the values and is able to determine
   * the top airport.
   */
  public static class TopBusyAirportReducer extends Reducer<Text, StringIntPair, Text, IntWritable> {

    @Override
    public void reduce(Text token, Iterable<StringIntPair> values,
        Context context) throws IOException, InterruptedException {
      String topAirport = null;
      int max = 0;

      for (StringIntPair v : values) {
        if (v.getSecond() > max) {
          max = v.getSecond();
          topAirport = v.getFirst();
        }
      }
      context.write(new Text(topAirport), new IntWritable(max));
    }
  }

  public int run(String[] args) throws Exception {

    GfxdDataSerializable.initTypes();
    Configuration conf = getConf();

    Path outputPath = new Path(args[0]);
    Path intermediateOutputPath = new Path(args[0] + "_int");
    String hdfsHomeDir = args[1];
    String tableName = args[2];

    outputPath.getFileSystem(conf).delete(outputPath, true);
    intermediateOutputPath.getFileSystem(conf).delete(intermediateOutputPath, true);

    conf.set(RowInputFormat.HOME_DIR, hdfsHomeDir);
    conf.set(RowInputFormat.INPUT_TABLE, tableName);
    conf.setBoolean(RowInputFormat.CHECKPOINT_MODE, false);

    Job job = Job.getInstance(conf, "Busy Airport Count");

    job.setInputFormatClass(RowInputFormat.class);

    // configure mapper and reducer
    job.setMapperClass(SampleMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    // Only have one reduce task so that all of the results from mapping are
    // processed in one place.
    job.setNumReduceTasks(1);

    // configure output
    TextOutputFormat.setOutputPath(job, intermediateOutputPath);
    job.setOutputFormatClass(TextOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    int rc = job.waitForCompletion(true) ? 0 : 1;
    if (rc == 0) {
      Job topJob = Job.getInstance(getConf(), "Top Busy Airport");

      // We want the task to run on a single VM
      topJob.setNumReduceTasks(1);

      // Set the inputs
      topJob.setInputFormatClass(TextInputFormat.class);
      TextInputFormat.addInputPath(topJob, intermediateOutputPath);

      // Set the mapper and reducer
      topJob.setMapperClass(TopBusyAirportMapper.class);
      topJob.setReducerClass(TopBusyAirportReducer.class);

      // Set the outputs
      TextOutputFormat.setOutputPath(topJob, outputPath);
      topJob.setOutputFormatClass(TextOutputFormat.class);
      topJob.setOutputKeyClass(Text.class);
      topJob.setOutputValueClass(IntWritable.class);

      topJob.setMapOutputKeyClass(Text.class);
      topJob.setMapOutputValueClass(StringIntPair.class);

      rc = topJob.waitForCompletion(true) ? 0 : 1;
    }
    return rc;
  }

  public static void main(String[] args) throws Exception {
    System.out.println("SampleApp.main() invoked with " + Arrays.toString(args));
    int rc = ToolRunner.run(new TopBusyAirport(), args);
    System.exit(rc);
  }
}

