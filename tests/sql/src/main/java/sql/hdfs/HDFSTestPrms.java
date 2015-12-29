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
package sql.hdfs;

import hydra.BasePrms;

public class HDFSTestPrms extends BasePrms {
  static {
     BasePrms.setValues(HDFSTestPrms.class);
  }  
  
  public static Long useRandomConfig;
  public static boolean useRandomConfig() {
    Long key = useRandomConfig;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false) );
    return val;
 }

  public static Long isCompactionTest;
  public static boolean isCompactionTest() {
    Long key = isCompactionTest;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false) );
    return val;
 }
  
  public static Long desiredMinorCompactions;
  public static int getDesiredMinorCompactions() {
    Long key = desiredMinorCompactions;
    int val = tasktab().intAt(key, tab().intAt(key, 50) );
    return val;
 }
  public static Long desiredMajorCompactions;
  public static int getDesiredMajorCompactions() {
    Long key = desiredMajorCompactions;
    int val = tasktab().intAt(key, tab().intAt(key, 1) );
    return val;
 }
  public static Long mapredVersion1;
  public static boolean useMapRedVersion1() {
    Long key = mapredVersion1;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false) );
    return val;
 }
  public static Long mapReduceDelaySec;
  public static int getMapReduceDelaySec() {
    Long key = mapReduceDelaySec;
    int val = tasktab().intAt(key, tab().intAt(key, 0) );
    return val;
 }
}
