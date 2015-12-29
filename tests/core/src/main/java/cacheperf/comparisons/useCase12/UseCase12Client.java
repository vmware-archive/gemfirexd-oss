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

package cacheperf.comparisons.useCase12;

import cacheperf.*;
import distcache.gemfire.GemFireCacheTestImpl;
import objects.*;

/**
 *
 *  UseCase12Client based upon cacheperf.CachePerfClient
 *  Hydra implementation of useCase12 benchmark test.
 *
 */

public class UseCase12Client extends CachePerfClient {

  //----------------------------------------------------------------------------
  //  New methods
  //----------------------------------------------------------------------------

  /**
   * TASK to register interest for invalidates.
   */
  public static void registerForInvalidatesTask() {
    UseCase12Client c = new UseCase12Client();
    c.initialize(REGISTERINTERESTS);
    c.registerForInvalidates();
  }
  private void registerForInvalidates() {
    if (this.cache instanceof GemFireCacheTestImpl) {
      long start = this.statistics.startRegisterInterest();
      ((GemFireCacheTestImpl)this.cache).getRegion()
        .registerInterestRegex(".*", false, false);
      this.statistics.endRegisterInterest(start,
                                          this.isMainWorkload,
                                          this.histogram);
    }
  }

  /**
   *  Task to perform same actions as the useCase12 benchmark
   *  Basically, it creates a key & EqStruct and puts this into the cache
   *  It then retrieves the previously written key/value pair, updates it
   *  and puts the updated version back into the cache.
   */
  public static void updateEqStructTask() {
    UseCase12Client c = new UseCase12Client();
    c.initialize( USECASE12_UPDATES );
    c.updateEqStruct();
  }

  private void updateEqStruct() {
    do {
      int key = getNextKey();
      executeTaskTerminator();
      executeWarmupTerminator();
      update( key );
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;
    } while (!executeBatchTerminator());
  }

  private void update( int i ) {
    Object key = ObjectHelper.createName( this.keyType, i );
    String objectType = CachePerfPrms.getObjectType();
    Object val = ObjectHelper.createObject( objectType, i );

    long start = this.statistics.startUseCase12Update();

    // put key, data pair
    this.cache.put( key, val );

    // get, update, put
    EqStruct eq = (EqStruct)this.cache.get( key );
    eq.update();
     
    this.cache.put( key, eq );

    this.statistics.endUseCase12Update(start, this.isMainWorkload, this.histogram);
  }
}
