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

package hydratest.version.partition;

import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import hydra.AsyncEventQueueHelper;
import java.util.Properties;

/**
 * Class for testing hydra version support.
 */
public class VersionHelper {

  protected static String getDeltaPropagation(Properties p) {
    return p.getProperty(DistributionConfig.DELTA_PROPAGATION_PROP_NAME);
  }
  protected static String getCloningEnabled(RegionAttributes ra) {
    return Boolean.valueOf(ra.getCloningEnabled()).toString();
  }
  protected static String getPartitionResolver(PartitionAttributes pa) {
    return pa.getPartitionResolver().toString();
  }
  protected static String getColocatedWith(PartitionAttributes pa) {
    return pa.getColocatedWith().toString();
  }
  protected static String getRecoveryDelay(PartitionAttributes pa) {
    return String.valueOf(pa.getRecoveryDelay());
  }
  protected static String getStartupRecoveryDelay(PartitionAttributes pa) {
    return String.valueOf(pa.getStartupRecoveryDelay());
  }
  protected static String getMultiuserAuthentication(Pool pool) {
    return String.valueOf(pool.getMultiuserAuthentication());
  }
  protected static String getPRSingleHopEnabled(Pool pool) {
    return String.valueOf(pool.getPRSingleHopEnabled());
  }
  protected static String createAsyncEventQueue(String queueConfig) {
    AsyncEventQueue queue = AsyncEventQueueHelper.createAndStartAsyncEventQueue(queueConfig);
    return AsyncEventQueueHelper.asyncEventQueueToString(queue);
  }
}
