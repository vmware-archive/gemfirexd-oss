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
package com.pivotal.pxf.gfxd.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

public class TestDataHelper {

  public static final int RECORDS_PER_SPLIT = 5;

  // First '/' is skipped because pxf adds it for inputdata's path
  // We may not need it if we decide only schemaname.tablename is what hawq user needs to provide while defining external table
  public static final String BASE_DIR = "ashetkar2/gemfire/gfxd/";

  public static final String SCHEMA_NAME = "schemaone";

  /**
   * region1: int order_id (key), int cust_id
   * region2: int cust_id (key), varchar cust_name, varchar cust_address
   * region3: int order_id (key), varchar order_desc, boolean order_shipped, int cust_id
   */
  public static final String[] TABLE_NAMES = new String[] {"region1", "region2", "region3"};

  /**
   * value, pxf-type
   * 23, integer
   * 1043, varchar
   * 16, boolean
   * [tableIndex][columnIndex][columnName, columnType, columnTypeName]
   */
  public static final String[][][] TABLE_META_DATA = new String[][][] {
      { { "order_id", "23", "integer"}, { "cust_id", "23", "integer"} },
      { { "cust_id", "23", "integer"}, { "cust_name", "1043", "varchar"}, { "cust_address", "1043", "varchar"} },
      { { "order_id", "23", "integer"}, { "order_desc", "1043", "varchar"}, { "order_shipped", "16", "boolean"}, { "cust_id", "23", "integer"} } };

  public static final String[] HOST_NAMES = new String[] {"hostname1", "hostname2", "hostname3", "hostname4", "hostname5"};

  private static final HashMap<String, InputSplit[]> REGION_SPLITS = new HashMap<String, InputSplit[]>();

  private static final HashMap<InputSplit, List<Object[]>> SPLIT_DATA = new HashMap<InputSplit, List<Object[]>>();

  static {
    initializeTestData();
  }

  private static void initializeTestData() {
    populateRegionSplits();
    populateSplitRecords();
  }
  
  public static Map<String, String> populateInputDataParam(int tableIndex) {
    return populateInputDataParam(tableIndex, 0, 1, "0");
  }

  public static Map<String, String> populateInputDataParam(int tableIndex,
      int segId, int segCount, String segs) {
    HashMap<String, String> map = new HashMap<String, String>();

    map.put("X-GP-ALIGNMENT", "4"); // Don't know what it is

    map.put("X-GP-SEGMENT-ID", String.valueOf(segId));
    map.put("X-GP-SEGMENT-COUNT", String.valueOf(segCount));
    map.put("X-GP-DATA-FRAGMENT", segs); // fragment index

    map.put("X-GP-HAS-FILTER", "0");
    // map.put("X-GP-FILTER", "");

    map.put("X-GP-FORMAT", "GPDBWritable");
    
    map.put("X-GP-URL-HOST", "hostname"); // Don't know what it is
    map.put("X-GP-URL-PORT", "30079"); // Don't know what it is
    
    map.put("X-GP-DATA-DIR", BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[tableIndex]);

    map.put("X-GP-ATTRS", String.valueOf(TABLE_META_DATA[tableIndex].length));
    for (int i = 0; i < TABLE_META_DATA[tableIndex].length; i++) {
      map.put("X-GP-ATTR-NAME" + i, TABLE_META_DATA[tableIndex][i][0]);
      map.put("X-GP-ATTR-TYPECODE" + i, TABLE_META_DATA[tableIndex][i][1]);
      map.put("X-GP-ATTR-TYPENAME" + i, TABLE_META_DATA[tableIndex][i][2]);
    }

    // map.put("X-GP-DATA-SCHEMA", "");
    map.put("X-GP-ACCESSOR", "GemFireXDAccessor");
    map.put("X-GP-RESOLVER", "GemFireXDResolver");
    
    return map;
  }

  private static void populateRegionSplits() {

    InputSplit[] splits1 = new InputSplit[1];
    InputSplit[] splits2 = new InputSplit[5];
    InputSplit[] splits3 = new InputSplit[10];

    // Currently, the details of the split (i.e. path, start, length and hostnames really don't matter
    // TABLE 0, FILES 1, SPLITS 1
    String path = "/" + BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[0] + "/data.hop";
    splits1[0] = getCombineSplit(new Path(path), 0L, 1024L, new String[]{HOST_NAMES[0], HOST_NAMES[1]});

    // TABLE 1, FILES 3, SPLITS 5
    path = "/" + BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[1] + "/data1.hop";
    splits2[0] = getCombineSplit(new Path(path), 0L, 512L, new String[]{HOST_NAMES[0], HOST_NAMES[1]});
    splits2[1] = getCombineSplit(new Path(path), 512L, 512L, new String[]{HOST_NAMES[0], HOST_NAMES[2]});

    path = "/" + BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[1] + "/data2.hop";
    splits2[2] = getCombineSplit(new Path(path), 1024L, 1024L, new String[]{HOST_NAMES[0], HOST_NAMES[3]});
    splits2[3] = getCombineSplit(new Path(path), 2048L, 2048L, new String[]{HOST_NAMES[1], HOST_NAMES[2]});

    path = "/" + BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[1] + "/data.hop";
    splits2[4] = getCombineSplit(new Path(path), 4096L, 512L, new String[]{HOST_NAMES[2], HOST_NAMES[3]});

    // TABLE 2, FILES 2, SPLITS 10
    path = "/" + BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[2] + "/data1.hop";
    splits3[0] = getCombineSplit(new Path(path), 0L, 1024L, new String[]{HOST_NAMES[0], HOST_NAMES[1]});
    splits3[1] = getCombineSplit(new Path(path), 1024L, 1024L, new String[]{HOST_NAMES[0], HOST_NAMES[2]});
    splits3[2] = getCombineSplit(new Path(path), 2048L, 1024L, new String[]{HOST_NAMES[0], HOST_NAMES[3]});
    splits3[3] = getCombineSplit(new Path(path), 3072L, 1024L, new String[]{HOST_NAMES[0], HOST_NAMES[1], HOST_NAMES[3]});
    path = "/" + BASE_DIR + SCHEMA_NAME + "." + TABLE_NAMES[2] + "/data.hop";
    splits3[4] = getCombineSplit(new Path(path), 4096L, 1024L, new String[]{HOST_NAMES[1], HOST_NAMES[2]});
    splits3[5] = getCombineSplit(new Path(path), 5120L, 1024L, new String[]{HOST_NAMES[1], HOST_NAMES[3]});
    splits3[6] = getCombineSplit(new Path(path), 6144L, 1024L, new String[]{HOST_NAMES[1], HOST_NAMES[2], HOST_NAMES[3]});
    splits3[7] = getCombineSplit(new Path(path), 7168L, 1024L, new String[]{HOST_NAMES[2], HOST_NAMES[3]});
    splits3[8] = getCombineSplit(new Path(path), 8192L, 1024L, new String[]{HOST_NAMES[2], HOST_NAMES[3], HOST_NAMES[0]});
    splits3[9] = getCombineSplit(new Path(path), 9216L, 1024L, new String[]{HOST_NAMES[3], HOST_NAMES[4]});

    REGION_SPLITS.put(TABLE_NAMES[0], splits1);
    REGION_SPLITS.put(TABLE_NAMES[1], splits2);
    REGION_SPLITS.put(TABLE_NAMES[2], splits3);
  }

  private static InputSplit getCombineSplit(Path path, long l, long m, String[] strings) {
    Path[] paths = {path};
    long[] offsets = {l};
    long[] lengths = {m};
    return new CombineFileSplit(null, paths, offsets, lengths, strings);
  }

  private static void populateSplitRecords() {
    String r = TABLE_NAMES[0];
    int i = 0;
    for (InputSplit split : getSplits(r)) {
      ArrayList<Object[]> list = new ArrayList<Object[]>();
      for(int j = 0; j < RECORDS_PER_SPLIT; ++j, ++i) {
        list.add(new Object[] {i, i+1000});
      }
      SPLIT_DATA.put(split, list);
      ++i;
    }

    r = TABLE_NAMES[1];
    for (InputSplit split : getSplits(r)) {
      ArrayList<Object[]> list = new ArrayList<Object[]>();
      for(int j = 0; j < RECORDS_PER_SPLIT; ++j, ++i) {
        list.add(new Object[] {i+1000, "cust_name_" + i, "cust_address_" + i} );
      }
      SPLIT_DATA.put(split, list);
      ++i;
    }

    r = TABLE_NAMES[2];
    for (InputSplit split : getSplits(r)) {
      ArrayList<Object[]> list = new ArrayList<Object[]>();
      for(int j = 0; j < RECORDS_PER_SPLIT; ++j, ++i) {
        list.add(new Object[] {i, "order_desc_" + i, Boolean.TRUE, i+1000} );
      }
      SPLIT_DATA.put(split, list);
      ++i;
    }
  }

  public TestDataHelper() {
  }

  public static InputSplit[] getSplits(String region) {
    return REGION_SPLITS.get(region);
  }

  public static List<Object[]> getSplitRecords(InputSplit split) {
    return SPLIT_DATA.get(split);
  }

  /**
   * Some serialization
   * @param data
   * @return
   */
  public static byte[] serialize(Object data) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(baos);
    
    if (data instanceof Object[]) {
      out.write(((Object[]) data).length);
      for (Object obj : (Object[]) data) {
        write(out, obj);
      }
    } else {
      write(out, data);
    }

    // Serialize it and return
    return baos.toByteArray();
  }

  private static void write(DataOutput out, Object data) throws IOException {
    if (data instanceof Integer) {
      out.writeInt((Integer)data);
    } else if (data instanceof String) {
      out.writeUTF((String)data);
    } else if (data instanceof Boolean) {
      out.writeBoolean((Boolean)data);
    } else {
      throw new IllegalArgumentException("Unknown type " + data);
    }
  }

  /**
   * Some deserialization
   * @param data
   * @return
   */
  public static Object deserialize(String region, byte[] bytes) throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInput in = new DataInputStream(bais);
    Object result = null;

    if (TestDataHelper.TABLE_NAMES[0].equals(region)) {
      result = in.readInt();
    } else if (TestDataHelper.TABLE_NAMES[1].equals(region)) {
      result = in.readInt();
    } else if (TestDataHelper.TABLE_NAMES[2].equals(region)) {
      result = in.readInt();
    } else {
      throw new Exception("Region is unknown " + region);
    }
    return result;
  }

  public static Object[] deserializeArray(String region, byte[] bytes) throws Exception {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInput in = new DataInputStream(bais);
    Object[] result = null;

    int values = in.readByte();
    result = new Object[values];
    if (TestDataHelper.TABLE_NAMES[0].equals(region)) {
      result[0] = in.readInt();
      result[1] = in.readInt();
    } else if (TestDataHelper.TABLE_NAMES[1].equals(region)) {
      result[0] = in.readInt();
      result[1] = in.readUTF();
      result[2] = in.readUTF();
    } else if (TestDataHelper.TABLE_NAMES[2].equals(region)) {
      result[0] = in.readInt();
      result[1] = in.readUTF();
      result[2] = in.readBoolean();
      result[3] = in.readInt();
    } else {
      throw new Exception("Region is unknown " + region);
    }
    return result;
  }

}
