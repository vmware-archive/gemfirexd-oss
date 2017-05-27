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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.Scope;


/**
 * This test will test that there are no unexpected behaviours
 * if the the region attributes are changed after starting it again.
 * 
 * The behaviour should be predictable
 * 
 * @author Mitul Bid
 *
 */
public class DiskRegionChangingRegionAttributesJUnitTest extends
    DiskRegionTestingBase
{

  public DiskRegionChangingRegionAttributesJUnitTest(String name) {
    super(name);
  }
  
  protected void setUp() throws Exception
  {
    super.setUp();
    props = new DiskRegionProperties();
    props.setDiskDirs(dirs);
    
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }
  

  private DiskRegionProperties props;
  
  private void createOverflowOnly(){
    props.setOverFlowCapacity(1);
    region = DiskRegionHelperFactory.getSyncOverFlowOnlyRegion(cache,props);
  }
  
  private void createPersistOnly(){
    region = DiskRegionHelperFactory.getSyncPersistOnlyRegion(cache,props, Scope.LOCAL);
  }
  
  private void createPersistAndOverflow(){
    region = DiskRegionHelperFactory.getSyncOverFlowAndPersistRegion(cache,props); 
  }
  
  public void testOverflowOnlyAndThenPersistOnly(){
    createOverflowOnly();
    put100Int();
    region.close();
    createPersistOnly();
    assertTrue(region.size()==0);
  }
  
  public void testPersistOnlyAndThenOverflowOnly(){
    createPersistOnly();
    put100Int();
    region.close();
    try {
      createOverflowOnly();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    //Asif Recreate the region so that it gets destroyed in teardown 
    //clearing up the old Oplogs
    createPersistOnly();
    
  }
  
  public void testOverflowOnlyAndThenPeristAndOverflow(){
    createOverflowOnly();
    put100Int();
    region.close();
    createPersistAndOverflow();
    assertTrue(region.size()==0);
  }
  
  public void testPersistAndOverflowAndThenOverflowOnly(){
    createPersistAndOverflow();
    put100Int();
    region.close();
    try {
      createOverflowOnly();
      fail("expected IllegalStateException");
    } catch (IllegalStateException expected) {
    }
    createPersistAndOverflow();
  }
  
 public void testPersistOnlyAndThenPeristAndOverflow(){
   createPersistOnly();
   put100Int();
   region.close();
   createPersistAndOverflow();
   assertTrue(region.size()==100);
  }
  
  public void testPersistAndOverflowAndThenPersistOnly(){
    createPersistAndOverflow();
    put100Int();
    region.close();
    createPersistOnly();
    assertTrue(region.size()==100);
  }
  
  
  
}

