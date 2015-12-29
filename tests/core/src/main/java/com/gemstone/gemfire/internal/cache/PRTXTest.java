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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.TXTest;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;

/**
 * @author sbawaska
 *
 */
public class PRTXTest extends TXTest {

  /**
   * @param name
   */
  public PRTXTest(String name) {
    super(name);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.TXTest#createRegion()
   */
  @Override
  protected void createRegion() throws Exception {
    AttributesFactory af = new AttributesFactory();
    af.setConcurrencyChecksEnabled(false);  // test validation expects this behavior
    af.setPartitionAttributes(new PartitionAttributesFactory()
      .setTotalNumBuckets(3).create());
    //this.region = this.cache.createRegion("PRTXTest", af.create());
    this.region = new PRWithLocalOps("PRTXTest", af.create(),
        null, this.cache, new InternalRegionArguments()
        .setDestroyLockFlag(true).setRecreateFlag(false)
        .setSnapshotInputStream(null).setImageTarget(null));
    ((PartitionedRegion)this.region).initialize(null, null, null);
    ((PartitionedRegion)this.region).postCreateRegion();
    this.cache.setRegionByPath(this.region.getFullPath(), (LocalRegion)this.region);
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.TXTest#checkUserAttributeConflict(com.gemstone.gemfire.internal.cache.TXManagerImpl)
   */
  @Override
  protected void checkUserAttributeConflict(CacheTransactionManager txMgrImpl) {
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.TXTest#checkSubRegionCollecection(com.gemstone.gemfire.internal.cache.LocalRegion)
   */
  @Override
  protected void checkSubRegionCollecection(Region reg1) {
  }
  @Override
  public void testTXAndQueries() throws CacheException, QueryException {
    // TODO fix this?
  }

  @Override
  public void testCollections() throws CacheException {
    // TODO make PR iterators tx aware
  }

  @Override
  public void testTxAlgebra() throws CacheException {
    // TODO Auto-generated method stub
  }

  public void testTxId() {
    AttributesFactory<Integer, String> af = new AttributesFactory<Integer, String>();
    af.setPartitionAttributes(new PartitionAttributesFactory<String, Integer>()
        .setTotalNumBuckets(2).create());
    Region<String, Integer> r = this.cache.createRegion("testTxId", af.create());
    r.put("one", 1);
    CacheTransactionManager mgr = this.cache.getTxManager();
    mgr.begin();
    r.put("two", 2);
    mgr.getTransactionId();
    mgr.rollback();
  }
}

class PRWithLocalOps extends PartitionedRegion {

  /**
   * @param regionname
   * @param ra
   * @param parentRegion
   * @param cache
   * @param internalRegionArgs
   */
  public PRWithLocalOps(String regionname, RegionAttributes ra,
      LocalRegion parentRegion, GemFireCacheImpl cache,
      InternalRegionArguments internalRegionArgs) {
    super(regionname, ra, parentRegion, cache, internalRegionArgs);
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#localDestroy(java.lang.Object, java.lang.Object)
   */
  @Override
  public void localDestroy(Object key, Object callbackArgument)
      throws EntryNotFoundException {
    super.destroy(key, callbackArgument);
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#localInvalidate(java.lang.Object, java.lang.Object)
   */
  @Override
  public void localInvalidate(Object key, Object callbackArgument)
      throws EntryNotFoundException {
    super.invalidate(key, callbackArgument);
  }
  /* (non-Javadoc)
   * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#localInvalidateRegion(java.lang.Object)
   */
  @Override
  public void localInvalidateRegion(Object callbackArgument) {
    super.invalidateRegion(callbackArgument);
  }
}
