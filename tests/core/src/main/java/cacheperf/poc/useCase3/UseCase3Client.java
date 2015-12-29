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

package cacheperf.poc.useCase3;

import cacheperf.*;
import com.useCase3.rds.model.trade.RDSCDSContract;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.types.*;
import hydra.*;
import java.util.*;
import perffmwk.*;

/**
 * Client for UseCase3 query testing.
 */
public class UseCase3Client extends CachePerfClient {

  private static Cache useCase3Cache;
  Random rng = new Random();

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * INITTASK to create the cache with the config corresponding to the VM type.
   */
  public static synchronized void openUseCase3CacheTask() {
    if (useCase3Cache == null) {
      useCase3Cache = CacheHelper.createCache(getVMType());
    }
  }
  
  /**
   * INITTASK to start the bridge server.
   */
  public static void startBridgeServerTask() {
    UseCase3Client c = new UseCase3Client();
    BridgeHelper.startBridgeServer(c.getVMType());
  }

  /**
   * INITTASK to create a region hierarchy.  Assumes that each region
   * configuration is prefixed with the VM type and suffixed with the region
   * name.
   */
  public static synchronized void createUseCase3RegionsTask() {
    UseCase3Client c = new UseCase3Client();
    c.initHydraThreadLocals();
    c.createUseCase3Regions();
    c.updateHydraThreadLocals();
  }
  private void createUseCase3Regions() {
    String prefix = getVMType();

    RegionAttributes ratts;
    ratts = RegionHelper.getRegionAttributes(prefix + "TRADE");
    Region r0 = RegionHelper.createRegion("TRADE", ratts);
    ratts = RegionHelper.getRegionAttributes(prefix + "CDS");
    Region r1 = r0.createSubregion("CDS", ratts);
    ratts = RegionHelper.getRegionAttributes(prefix + "ALL");
    Region r2 = r1.createSubregion("ALL", ratts);
    ratts = RegionHelper.getRegionAttributes(prefix + "CALYPSO");
    Region r3 = r2.createSubregion("CALYPSO", ratts);
    if (prefix.equals("edge")) {
      Pool pool = PoolHelper.createPool("poolGLOBAL");
    }
    ratts = RegionHelper.getRegionAttributes(prefix + "GLOBAL");
    Region r4 = r3.createSubregion("GLOBAL", ratts);
  }

  /**
   * INITTASK to create indexes.
   */
  public static void createUseCase3IndexesTask() throws QueryException {
    UseCase3Client c = new UseCase3Client();
    c.createUseCase3Indexes();
  }
  private void createUseCase3Indexes() throws QueryException {
    QueryService qs = CacheHelper.getCache().getQueryService();
    qs.createIndex("TrdI_RDSCDSContract_Index", IndexType.FUNCTIONAL,
                   "TrdI", "/TRADE/CDS/ALL/CALYPSO/GLOBAL");
    qs.createIndex("TrdStatus_RDSCDSContract_Index", IndexType.FUNCTIONAL,
                   "TrdStatus", "/TRADE/CDS/ALL/CALYPSO/GLOBAL");
    qs.createIndex("Strtgy_RDSCDSContract_Index", IndexType.FUNCTIONAL,
                   "Strtgy", "/TRADE/CDS/ALL/CALYPSO/GLOBAL");
    qs.createIndex("CrCrvCrcy_RDSCDSContract_Index", IndexType.FUNCTIONAL,
                   "CrCrvCrcy", "/TRADE/CDS/ALL/CALYPSO/GLOBAL");
    qs.createIndex("CrCrvSubord_RDSCDSContract_Index", IndexType.FUNCTIONAL,
                   "CrCrvSubord", "/TRADE/CDS/ALL/CALYPSO/GLOBAL");
  }

  public static void createUseCase3DataTask() {
    UseCase3Client c = new UseCase3Client();
    c.initialize(CREATES);
    c.createUseCase3Data();
  }
  private void createUseCase3Data() {
    Region r = useCase3Cache.getRegion("/TRADE/CDS/ALL/CALYPSO/GLOBAL");
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      createUseCase3(r, key);
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }
  private void createUseCase3(Region r, int i) {
    long start = this.statistics.startCreate();
    r.put(i, Contract.init(i));
    this.statistics.endCreate(start, this.isMainWorkload, this.histogram);
  }

  /**
   * TASK to do queries.
   */
  public static void queryUseCase3DataTask() throws QueryException {
    UseCase3Client c = new UseCase3Client();
    c.initialize(QUERIES);
    c.queryUseCase3Data();
  }
  private void queryUseCase3Data() throws QueryException {
    Region r = useCase3Cache.getRegion("/TRADE/CDS/ALL/CALYPSO/GLOBAL");
    String oql;
    boolean verbose = UseCase3Prms.logQueryResults();
    int numPuts = UseCase3Prms.getNumPutsBetweenQueries();
    int ms = CachePerfPrms.getSleepMs();

    do {
      int trdI = getNextKey();

      executeTaskTerminator();
      executeWarmupTerminator();

      oql = "SELECT * FROM /TRADE/CDS/ALL/CALYPSO/GLOBAL where TrdI="
          +  trdI + "L";
      queryUseCase3(r, oql, verbose);

      putUseCase3(r, numPuts);
      MasterController.sleepForMs(ms);

      oql = "SELECT * FROM /TRADE/CDS/ALL/CALYPSO/GLOBAL where Strtgy='A"
          + trdI + "'";
      queryUseCase3(r, oql, verbose);

      putUseCase3(r, numPuts);
      MasterController.sleepForMs(ms);

    } while (!executeBatchTerminator());
  }
  private void queryUseCase3(Region r, String oql, boolean verbose)
  throws QueryException {
    if (verbose) {
      Log.getLogWriter().info("OQL=" + oql);
    }
    long start = this.statistics.startQuery();
    SelectResults q = r.query(oql);
    this.statistics.endQuery(start, this.isMainWorkload, this.histogram);
    if (verbose) {
      for (Iterator it = q.iterator(); it.hasNext();) {
        Log.getLogWriter().info("Object: " + it.next());
      }
    }
  }
  private void putUseCase3(Region r, int numPuts) {
    for (int i = 0; i < numPuts; i++) {
      int key = getNextKey();
      long start = this.statistics.startPut();
      r.put(key, Contract.init(key));
      this.statistics.endPut(start, this.isMainWorkload, this.histogram);
    }
  }

  /**
   * CLOSETASK to close the cache.
   */
  public static synchronized void closeUseCase3CacheTask() {
    if (useCase3Cache != null) {
      useCase3Cache.close();
      useCase3Cache = null;
    }
  }

  private static String getVMType() {
    String type = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    if (type == null) {
      String s = "Not a hydra client VM";
      throw new HydraConfigException(s);
    } else if (type.startsWith("bridge")) {
      return "bridge";
    } else if (type.startsWith("edge")) {
      return "edge";
    } else {
      String s = "Unknown VM type: " + type;
      throw new HydraConfigException(s);
    }
  }
}
