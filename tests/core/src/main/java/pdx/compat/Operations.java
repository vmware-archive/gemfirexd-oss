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
package pdx.compat;

import hydra.CacheHelper;
import hydra.Log;

import java.util.Set;

import pdx.PdxBB;
import pdx.PdxTest;
import util.BaseValueHolder;
import util.OperationsClient;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionedRegionStorageException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.persistence.PartitionOfflineException;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;

public class Operations extends OperationsClient {

  public static void HydraTask_doOperations() {
    PdxTest.initClassLoader();
    Operations ops = new Operations();
    ops.initializeOperationsClient();
    Cache theCache = CacheHelper.getCache();
    if (theCache == null) {  // can happen during HA
      return; 
    }
    Set<Region<?, ?>> rootRegions = theCache.rootRegions();
    for (Region aRegion: rootRegions) {
      ops.doEntryOperations(aRegion);
    }
  }

  /** Return a value for the given key
   */
  public BaseValueHolder getValueForKey(Object key) {
    return PdxCompatTest.getValueForKey(key);
  }

  protected BaseValueHolder getUpdateObject(Region aRegion, Object key) {
    Object cacheObj = aRegion.get(key);
    BaseValueHolder anObj = PdxTest.toValueHolder(cacheObj);
    BaseValueHolder newObj = null;
    if (anObj == null) {
      newObj = getValueForKey(key);
    } else {
      newObj = anObj.getAlternateValueHolder(randomValues);
    }
    return newObj;
  }

  /** Reimplemented to allow CacheClosedExceptions while stopping jvms
   * 
   */
  protected void doEntryOperations(Region aRegion) {
    try {
      Log.getLogWriter().info(aRegion.getFullPath() + " is size " + aRegion.size());
      super.doEntryOperations(aRegion);
    } catch (CacheClosedException e) {
      if (PdxBB.getBB().getSharedCounters().read(PdxBB.shutDownAllInProgress) >= 1) {
        Log.getLogWriter().info("Allowed " + e + " during shutDownAll");
      } else {
        throw e;
      }
    } catch (RegionDestroyedException e) {
      if (PdxBB.getBB().getSharedCounters().read(PdxBB.shutDownAllInProgress) >= 1) {
        Log.getLogWriter().info("Allowed " + e + " during shutDownAll");
      } else {
        throw e;
      }
    } catch (DistributedSystemDisconnectedException e) {
      if (PdxBB.getBB().getSharedCounters().read(PdxBB.shutDownAllInProgress) >= 1) {
        Log.getLogWriter().info("Allowed " + e + " during shutDownAll");
      } else {
        throw e;
      }
    } catch (PartitionedRegionStorageException e) {
      // while restarting after shutDownAll, if the first jvm up and running ops is the "proxy"
      // jvm (the jvm with all accessors and empty regions), then there is no storage available
      // to host data, so allow an exception to occur during ops if we are restarting after a
      // shutDownAll
      if (e.toString().indexOf(" Unable to find any members to host a bucket") >= 0) {
        if ((PdxBB.getBB().getSharedCounters().read(PdxBB.restartingAfterShutDownAll) >= 1) ||
            (PdxBB.getBB().getSharedCounters().read(PdxBB.shutDownAllInProgress) >= 1)) {
          Log.getLogWriter().info("Allowed " + e + " while shutting down or restarting after shutDownAll");
        } else {
          throw e;
        }
      } else {
        throw e;
      }
    } catch (PartitionOfflineException e) {
      if ((PdxBB.getBB().getSharedCounters().read(PdxBB.restartingAfterShutDownAll) >= 1) ||
          (PdxBB.getBB().getSharedCounters().read(PdxBB.shutDownAllInProgress) >= 1)) {
        Log.getLogWriter().info("Allowed " + e + " while shutting down or restarting after shutDownAll");
      } else {
        PartitionAttributes prAttr = aRegion.getAttributes().getPartitionAttributes();
        if (prAttr != null) {
          if (prAttr.getRedundantCopies() == 0) { // with RC 0 we can allow PartitionOfflineException as jvms go down
            Log.getLogWriter().info("Allowed " + e + " because of 0 redundantCopies");
          } else {
            throw e;
          }
        } else {
          throw e;
        }
      }
    }

  }
}
