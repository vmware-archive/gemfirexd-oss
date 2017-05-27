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
package com.gemstone.gemfire.cache.query.partitioned;

import junit.framework.TestCase;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;

import io.snappydata.test.dunit.DistributedTestBase;

/**
 * Test verifies Region#query()for PartitionedRegion on a single VM with
 * Region#destroyRegion() being called on the same with some delay.
 * 
 * @author pbatra
 * 
 */
public class PRQueryCacheClosedJUnitTest extends TestCase
{
  // PR Region name
  static final String regionName = "portfolios";

  // Local Region name
  static final String localRegionName = "localPortfolios";

  // redundancy level for PR
  static final int redundancy = 0;

  // local max memory for PR
  static final String localMaxMemory = "100";

  LogWriter logger = null;

  boolean encounteredException = false;

  static final int dataSize = 100;

  static final int delayCC = 1500;

  static final int delayQuery = 1000;

  public PRQueryCacheClosedJUnitTest(String arg0) {
    super(arg0);
  }

  protected void setUp() throws Exception {
    super.setUp();
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }

  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }

  /**
   * Tests the execution of query on a PartitionedRegion created on a single
   * data store. <br>
   * 1. Creates a PR with redundancy=0 on a single VM. <br>
   * 2. Puts some test Objects in cache.<br>
   * 3. Create a Thread and fire queries on the data and verifies the result.<br>
   * 4. Create another Thread and call cache#close()<br>
   * 5. Recreates the cache after a delay of 1500ms
   * 
   * 
   * @throws Exception
   */
  public void testQueryOnSingleDataStoreWithCacheClose() throws Exception
  {

    logger
        .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Test Started  ");

    logger
        .info("PRQueryRegionDestroyedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: creating PR Region ");

    final Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, localMaxMemory, redundancy);

    final Region localRegion = PartitionedRegionTestHelper
        .createLocalRegion(localRegionName);

    final StringBuffer errorBuf = new StringBuffer("");

    PortfolioData[] portfolios = new PortfolioData[dataSize];

    try {
      for (int j = 0; j < dataSize; j++) {
        portfolios[j] = new PortfolioData(j);
      }
      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: populating PortfolioData into the PR Datastore  ");

      populateData(region, portfolios);

      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: populating PortfolioData into the PR Datastore  ");

      populateData(localRegion, portfolios);
      final String[] queryString = { "ID = 0 OR ID = 1", "ID > 4 AND ID < 9",
          "ID = 5", "ID < 5 ", "ID <= 5" };

      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Creating a Thread which will fire queries on the datastore");

      Thread t1 = new Thread(new Runnable() {
        public void run()
        {
          final String expectedCacheClosedException = CacheClosedException.class
              .getName();

          logger.info("<ExpectedException action=add>"
              + expectedCacheClosedException + "</ExpectedException>");

          for (int i = 0; i < queryString.length; i++) {

            try {

              SelectResults resSetPR = region.query(queryString[i]);
              
              SelectResults resSetLocal = localRegion.query(queryString[i]);
              
              String failureString=PartitionedRegionTestHelper.compareResultSets(resSetPR,
                  resSetLocal);
              Thread.sleep(delayQuery);
              if(failureString!=null){
                errorBuf.append(failureString);
                throw (new Exception(failureString));
                
              }

            }
            catch (InterruptedException ie) {
              fail("interrupted");

            }

            catch (CancelException cce) {
              logger
                  .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: CancelException as Expected "
                      + cce);

            }
            // it's also possible to get a RegionNotFoundException            
            catch (RegionNotFoundException rnfe) {
              logger
              .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: RegionNotFoundException as Expected "
                    + rnfe);
            }
            
            
            catch (Exception qe) {
              logger
                  .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Unexpected Exception "
                      + qe);

              encounteredException = true;
              errorBuf.append(DistributedTestBase.getStackTrace(qe));

            }

          }
          logger.info("<ExpectedException action=remove>"
              + expectedCacheClosedException + "</ExpectedException>");

        }
      });
      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Creating a Thread which will call cache.close() on the datastore ");

      Thread t2 = new Thread(new Runnable() {
        public void run()
        {
          PartitionedRegionTestHelper.closeCache();
          try {
            Thread.sleep(delayCC);
          }
          catch (InterruptedException ie) {
            fail("interrupted");
          }
         PartitionedRegionTestHelper.createCache();

        }

      });

      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Initiating the  Threads");

      t1.start();
      t2.start();

      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Waiting for the Threads to join ");

      DistributedTestBase.join(t1, 30 * 1000, null);
      DistributedTestBase.join(t2, 30 * 1000, null);
      logger
          .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: checking for any Unexpected Exception's occured");

      assertFalse(
          "PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Exception occured in Query-thread: " + errorBuf,
          encounteredException);
    }
    catch (Exception e) {
      e.printStackTrace();
      fail("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Test failed because of exception "
          + e);

    }

    logger
        .info("PRQueryCacheClosedJUnitTest#testQueryOnSingleDataStoreWithCacheClose: Test Ended");

  }

  /**
   * Populates the region with the Objects stores in the data Object array.
   * 
   * @param region
   * @param data
   */
  private void populateData(Region region, Object[] data)
  {
    logger
        .info("PRQueryCacheClosedJUnitTest#populateData: Populating Data in the PR Region ");
    for (int j = 0; j < data.length; j++) {
      region.put(new Integer(j), data[j]);
    }
  }
}
