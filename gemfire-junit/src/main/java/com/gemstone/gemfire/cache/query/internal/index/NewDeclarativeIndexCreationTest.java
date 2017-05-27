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
package com.gemstone.gemfire.cache.query.internal.index;

import java.util.Collection;
import java.util.Properties;

import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheXmlException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * @author shobhit
 *
 * @since 6.6.1
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NewDeclarativeIndexCreationTest extends TestCase {

  private Cache cache = null;

  public NewDeclarativeIndexCreationTest(String testName) {
    super(testName);
  }

  protected void setUp() throws Exception {
    //Read the Cache.xml placed in test.lib folder
    Properties props = new Properties();
    String path = System.getProperty("INDEXQUERYXMLFILE");
    //String path = "/home/shobhit/gemfire/branches/gemfire66_maint/tests/lib/cachequeryindex.xml";
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
    TestSuite suite = new TestSuite(NewDeclarativeIndexCreationTest.class);
    return suite;
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }
  public void test000AsynchronousIndexCreatedOnRoot_PortfoliosRegion() {
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
          .fail("NewDeclarativeIndexCreationTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  public void test001SynchronousIndexCreatedOnRoot_StringRegion() {
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
          .fail("NewDeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
        Assert
            .fail("NewDeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }

  public void test002SynchronousIndexCreatedOnRootRegion() {
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
          .fail("NewDeclarativeIndexCreationTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }
  

  // Index creation tests for new DTD changes for Index tag for 6.6.1 with no function/primary-key tag
  public void test003AsynchronousIndexCreatedOnPortfoliosRegionWithNewDTD() {
    Region root = cache.getRegion("/root/portfolios2");
    IndexManager im = IndexUtils.getIndexManager(root, true);
    Collection coll = im.getIndexes();
    if (coll.size() > 0) {
      Assert.assertTrue(true);
      System.out.println("List of indexes= " + im.toString());
      RegionAttributes ra = root.getAttributes();
      Assert.assertTrue(!ra.getIndexMaintenanceSynchronous());
    } else
      Assert
          .fail("NewDeclarativeIndexCreationTest::testAsynchronousIndexCreatedOnRoot_PortfoliosRegion:No index found in the root region");
  }

  public void test004SynchronousIndexCreatedOnStringRegionWithNewDTD() {
    Region root = cache.getRegion("/root/string2");
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
          .fail("NewDeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion Region:No index found in the root region");
    root = cache.getRegion("/root/string1");
    im = IndexUtils.getIndexManager(root, true);
    if (!im.isIndexMaintenanceTypeSynchronous())
        Assert
            .fail("DeclarativeIndexCreationTest::testSynchronousIndexCreatedOnRoot_StringRegion: The index update type not synchronous if no index-update-type attribuet specified in cache.cml");
  }
  
  public void tes005tIndexCreationExceptionOnRegionWithNewDTD() {
    if (cache != null && !cache.isClosed()) cache.close();
  //Read the Cache.xml placed in test.lib folder
    Properties props = new Properties();
    String path = System.getProperty("INDEXQUERYXMLWITHERRORFILE");
    //String path = "/home/shobhit/gemfire/branches/gemfire66_maint/tests/lib/cachequeryindexwitherror.xml";
    props.setProperty("cache-xml-file", path);
    //props.setProperty("cache-xml-file","D:\\program
    // files\\eclipse\\workspace\\Gemfire\\cache.xml");
    DistributedSystem ds = DistributedSystem.connect(props);
    try {
      Cache cache = CacheFactory.create(ds);
    } catch (CacheXmlException e) {
      e.printStackTrace();
      if (!e.getCause().getMessage().contains("CacheXmlParser::endIndex:Index creation attribute not correctly specified.")) {
        Assert.fail("NewDeclarativeIndexCreationTest::setup: Index creation should have thrown exception for index on /root/portfolios3 region.");
      }
      return;
    }
  }
}
