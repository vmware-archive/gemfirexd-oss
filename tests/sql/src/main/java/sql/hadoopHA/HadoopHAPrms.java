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
package sql.hadoopHA;

import hydra.BasePrms;

public class HadoopHAPrms extends BasePrms {

/** (boolean) expectExceptions
 *  Used as a task specific param (for example when we attempt to createHDFSStores before Hadoop started)
 *  Defaults to false.
 */
  public static Long expectExceptions;
  public static boolean expectExceptions() {
    Long key = expectExceptions;
    boolean val = tasktab().booleanAt( key, false);
    return val;
  }

  public static Long recycleDataStores;
  public static boolean recycleDataStores() {
    Long key = recycleDataStores;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false) );
    return val;
  }

/** (String) Indicates the gemfirexd host description to be used when determining
 *  which hosts are involved in dropConnections (for network partition tests).
 *  Defaults to 'datastorehost1'.
 */
  public static Long gemfirexdHostDescription;
  public static String gemfirexdHostDescription() {
    Long key = gemfirexdHostDescription;
    String val = tasktab().stringAt(key, tab().stringAt(key, "datastorehost1"));
    return val;
  }

/** (String) Indicates the gemfirexd host description to be used when determining
 *  which hosts are involved in dropConnections (for network partition tests).
 *  Defaults to 'namenode'.
 *  Choices: namenode, datanode.
 */
  public static Long hdfsComponentDescription;
  public static String hdfsComponentDescription() {
    Long key = hdfsComponentDescription;
    String val = tasktab().stringAt(key, tab().stringAt(key, "namenode"));
    return val;
  }

  static {
     BasePrms.setValues(HadoopHAPrms.class);
  }  
}
