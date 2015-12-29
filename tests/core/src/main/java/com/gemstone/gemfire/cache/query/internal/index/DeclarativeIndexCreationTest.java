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
/*
 * Created on Apr 19, 2005
 *
 */
package com.gemstone.gemfire.cache.query.internal.index;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.query.internal.index.IndexManager;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;
//import com.gemstone.gemfire.internal.cache.LocalRegion;
//import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
//import com.gemstone.gemfire.cache.query.*;
//import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * @author asifs
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
public class DeclarativeIndexCreationTest extends TestCase {

  private Cache cache = null;

  public DeclarativeIndexCreationTest(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
    //Read the Cache.xml placed in test.lib folder
    Properties props = new Properties();
    String path = System.getProperty("QUERYXMLFILE");
    props.setProperty("cache-xml-file", path);
    //props.setProperty("cache-xml-file","D:\\program
    // files\\eclipse\\workspace\\Gemfire\\cache.xml");
    DistributedSystem ds = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds);
  }

  protected void tearDown() throws Exception {
    if (!cache.isClosed()) cache.close();
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(DeclarativeIndexCreationTest.class);
    return suite;
  }

  public void testAsynchronousIndexCreatedOnRoot_PortfoliosRegion() {
    Region root = cache.getRegion("/root/portfolios");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(!ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("DeclarativeIndexCreationTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  public void testSynchronousIndexCreatedOnRoot_StringRegion() {
    Region root = cache.getRegion("/root/string");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    ;
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("DeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
        Assert
            .fail("DeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }

  public void testSynchronousIndexCreatedOnRootRegion() {
    Region root = cache.getRegion("/root");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("DeclarativeIndexCreationTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }
}
