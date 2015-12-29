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
package hdfs;

import hydra.BasePrms;

public class HDFSPrms extends BasePrms {

/** (boolean) whether or not to use an existing cluster (e.g. Isilon)
 *  Defaults to false.
 */
public static Long useExistingCluster;
public static boolean useExistingCluster() {
  Long key = useExistingCluster;
  boolean val = tasktab().booleanAt( key, tab().booleanAt(key, false) );
  return val;
}

/** (boolean) When running with an existing cluster (vs. starting the hadoop cluster),
 *  start mapreduce hadoop components (ResourceManager and NodeManager).
 *  Used with Isilon HDFS File System boxes (since we must still run the mapreduce components)
 *  Defaults to true.
 */
public static Long manageMapReduceComponents;
public static boolean manageMapReduceComponents() {
  Long key = manageMapReduceComponents;
  boolean val = tasktab().booleanAt( key, tab().booleanAt(key, false) );
  return val;
}

/** (String) fully qualified name for MapReduce job to execute
 *  For example, hdfs.mapreduce.KnownKeysMRv1
 */
public static Long mapReduceClassName;
public static String getMapReduceClassName() {
  Long key = mapReduceClassName;
  String val = tasktab().stringAt( key, tab().stringAt(key, null) );
  return val;
}

/** (int) The number of seconds to wait before stopping the Hadoop Cluster.
 *  default is 0
 */
public static Long hadoopStopWaitSec;
public static int hadoopStopWaitSec() {
  Long key = hadoopStopWaitSec;
  int val = tasktab().intAt( key, tab().intAt(key, 0) );
  return val;
}

/** (int) The number of seconds to wait before starting the Hadoop Cluster.
 *  default is 0
 */
public static Long hadoopStartWaitSec;
public static int hadoopStartWaitSec() {
  Long key = hadoopStartWaitSec;
  int val = tasktab().intAt( key, tab().intAt(key, 0) );
  return val;
}

/** (int) The number of seconds to wait before returning from recycling the Hadoop Cluster.
 *  default is 0
 */
public static Long hadoopReturnWaitSec;
public static int hadoopReturnWaitSec() {
  Long key = hadoopReturnWaitSec;
  int val = tasktab().intAt( key, tab().intAt(key, 0) );
  return val;
}


// ================================================================================
static {
   BasePrms.setValues(HDFSPrms.class);
}

}
