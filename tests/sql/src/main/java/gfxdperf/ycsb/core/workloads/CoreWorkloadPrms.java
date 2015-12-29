/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All Rights Reserved.
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
package gfxdperf.ycsb.core.workloads;

import hydra.BasePrms;
import hydra.HydraConfigException;

/**
 * A class used to store keys for test configuration settings.
 */
public class CoreWorkloadPrms extends BasePrms {

  static {
    setValues(CoreWorkloadPrms.class);
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Number of fields in <code>usertable</code>. Defaults to 10.
   */
  public static Long fieldCount;

  public static int getFieldCount() {
    Long key = fieldCount;
    return tasktab().intAt(key, tab().intAt(key, 10));
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * First field number out of the {@link #fieldCount} fields to consider when
   * choosing fields in <code>usertable</code>. Defaults to 0.
   */
  public static Long fieldStart;

  public static int getFieldStart() {
    Long key = fieldStart;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

//------------------------------------------------------------------------------

  private static final String DEFAULT_FIELD_LENGTH_DISTRIBUTION = "constant";

  public static enum FieldLengthDistribution {
    constant, uniform, zipfian, histogram
    ;
  }

  /**
   * (String)
   * Name of a field length distribution scheme found in {@link
   * #FieldLengthDistribution} used to determine the maximum field length.
   * Defaults to {@link #constant}. If "histogram", the field length is read
   * from {@link #fieldLengthHistogramFile}. Otherwise, it is determined by
   * {@link #fieldLength}.
   */
  public static Long fieldLengthDistribution;

  public static FieldLengthDistribution getFieldLengthDistribution() {
    Long key = fieldLengthDistribution;
    String val = tasktab().stringAt(key, tab().stringAt(key,
                                    DEFAULT_FIELD_LENGTH_DISTRIBUTION));
    for (FieldLengthDistribution d : FieldLengthDistribution.values()) {
      if (val.equalsIgnoreCase(d.toString())) {
        return d;
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s);
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Length of a field, in bytes. Defaults to 100.
   */
  public static Long fieldLength;

  public static int getFieldLength() {
    Long key = fieldLength;
    return tasktab().intAt(key, tab().intAt(key, 100));
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * File containing a field length histogram. Defaults to null. Used only if
   * {@link #fieldLengthDistribution} is "histogram".
   */
  public static Long fieldLengthHistogramFile;

  public static String getFieldLengthHistogramFile() {
    Long key = fieldLengthHistogramFile;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

//------------------------------------------------------------------------------

  private static final String DEFAULT_INSERT_ORDER = "hashed";

  public static enum InsertOrder {
    /** records are inserted in order by key */
    ordered,
    /** records are inserted in hashed order */
    hashed
    ;
  }

  /**
   * (String)
   * Name of an insert order scheme found in {@link #InsertOrder} used to
   * order keys being inserted. Defaults to {@link #DEFAULT_INSERT_ORDER}.
   */
  public static Long insertOrder;

  public static InsertOrder getInsertOrder() {
    Long key = insertOrder;
    String val = tasktab().stringAt(key, tab().stringAt(key,
                                    DEFAULT_INSERT_ORDER));
    for (InsertOrder i : InsertOrder.values()) {
      if (val.equalsIgnoreCase(i.toString())) {
        return i;
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s);
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Proportion of operations that should be inserts. For example, 0.10 would
   * produce 10% inserts. The sum of all proportions should be 1.00. Defaults
   * to 0.
   */
  public static Long insertProportion;

  public static double getInsertProportion() {
    Long key = insertProportion;
    return getProportion(key);
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Maximum number of records to scan. Defaults to 1000.
   */
  public static Long maxScanLength;

  public static int getMaxScanLength() {
    Long key = maxScanLength;
    return tasktab().intAt(key, tab().intAt(key, 1000));
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to read all fields of a record or just one. Defaults to true.
   */
  public static Long readAllFields;

  public static boolean getReadAllFields() {
    Long key = readAllFields;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Proportion of operations that should read, modify, and write a record.
   * For example, 0.50 would produce 50% read-modify-write operations. The
   * sum of all proportions should be 1.00. Defaults to 0.
   */
  public static Long readModifyWriteProportion;

  public static double getReadModifyWriteProportion() {
    Long key = readModifyWriteProportion;
    return getProportion(key);
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Proportion of operations that should be reads. For example, 0.95 would
   * produce 95% reads. The sum of all proportions should be 1.00. Defaults
   * to 0.
   */
  public static Long readProportion;

  public static double getReadProportion() {
    Long key = readProportion;
    return getProportion(key);
  }

//------------------------------------------------------------------------------

  /**
   * (int)
   * Number of records in <code>usertable</code>.
   */
  public static Long recordCount;

  public static int getRecordCount() {
    Long key = recordCount;
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    if (val < 0) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

//------------------------------------------------------------------------------

  private static final String DEFAULT_REQUEST_DISTRIBUTION = "uniform";

  public static enum RequestDistribution {
    uniform, zipfian, latest, exponential, hotspot
    ;
  }

  /**
   * (String)
   * Name of a request distribution scheme found in {@link #RequestDistribution}
   * used to allocate keys to threads doing YCSB workloads. Defaults to {@link
   * #DEFAULT_REQUEST_DISTRIBUTION}.
   */
  public static Long requestDistribution;

  public static RequestDistribution getRequestDistribution() {
    Long key = requestDistribution;
    String val = tasktab().stringAt(key, tab().stringAt(key,
                                    DEFAULT_REQUEST_DISTRIBUTION));
    for (RequestDistribution r : RequestDistribution.values()) {
      if (val.equalsIgnoreCase(r.toString())) {
        return r;
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s);
  }

//------------------------------------------------------------------------------

  private static final String DEFAULT_SCAN_LENGTH_DISTRIBUTION = "uniform";

  public static enum ScanLengthDistribution {
    uniform, zipfian
    ;
  }

  /**
   * (String)
   * Name of a scan length distribution scheme found in {@link
   * #ScanLengthDistribution} choose records for scans in YCSB workloads.
   * Defaults to {@link #DEFAULT_SCAN_LENGTH_DISTRIBUTION}.
   */
  public static Long scanLengthDistribution;

  public static ScanLengthDistribution getScanLengthDistribution() {
    Long key = scanLengthDistribution;
    String val = tasktab().stringAt(key, tab().stringAt(key,
                                    DEFAULT_SCAN_LENGTH_DISTRIBUTION));
    for (ScanLengthDistribution d : ScanLengthDistribution.values()) {
      if (val.equalsIgnoreCase(d.toString())) {
        return d;
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s);
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Proportion of operations that should be scans. For example, 0.25 would
   * produce 25% updates. The sum of all proportions should be 1.00. Defaults
   * to 0.
   */
  public static Long scanProportion;

  public static double getScanProportion() {
    Long key = scanProportion;
    return getProportion(key);
  }

//------------------------------------------------------------------------------

  /**
   * (String)
   * Name of the table. Defaults to "usertable".
   */
  public static Long tableName;

  public static String getTableName() {
    Long key = tableName;
    return tasktab().stringAt(key, tab().stringAt(key, "usertable"));
  }

//------------------------------------------------------------------------------

  /**
   * (double)
   * Proportion of operations that should be updates. For example, 0.25 would
   * produce 25% updates. The sum of all proportions should be 1.00. Defaults
   * to 0.
   */
  public static Long updateProportion;

  public static double getUpdateProportion() {
    Long key = updateProportion;
    return getProportion(key);
  }

//------------------------------------------------------------------------------

  /**
   * (boolean)
   * Whether to write all fields of a record or just one. Defaults to false.
   */
  public static Long writeAllFields;

  public static boolean getWriteAllFields() {
    Long key = writeAllFields;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

//------------------------------------------------------------------------------

  private static double getProportion(Long key) {
    double val = tasktab().doubleAt(key, tab().doubleAt(key, 0));
    if (val < 0 || val > 1) {
      String s = "Illegal value for " + nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }
}
