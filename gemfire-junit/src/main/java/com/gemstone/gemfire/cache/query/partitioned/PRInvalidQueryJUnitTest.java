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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.data.PortfolioData;
import com.gemstone.gemfire.internal.cache.PartitionedRegionTestHelper;

/**
 * Class verifies Region#query(Select Query) API for PartitionedRegion on a
 * single VM.,As region#query doesn't support Select Query for Local Querying it
 * should throw QueryInvalidException
 * 
 * @author pbatra
 * 
 */
public class PRInvalidQueryJUnitTest extends TestCase
{
  String regionName = "Portfolios";

  LogWriter logger = null;

  public PRInvalidQueryJUnitTest(String arg0) {
    super(arg0);
  }

  protected void setUp() throws Exception
  {
    super.setUp();
    if (logger == null) {
      logger = PartitionedRegionTestHelper.getLogger();
    }
  }

  protected void tearDown() throws Exception
  {
    super.tearDown();
  }

  /**
   * Tests the execution of an Invalid query <br>
   * (of the nature Select Distinct * from /Portfolios ) <br>
   * on a PartitionedRegion created on a single data store. <br>
   * 1. Creates a PR with redundancy=0 on a single VM.<br>
   * 2. Puts some test Objects in cache.<br>
   * 3. Fires querie on the data and verifies the result.<br>
   * 4. Since region#query() doesn't support this type of query syntax it should
   * throw QueryInvalidException <br>
   * 
   * @throws Exception
   */
  public void testInvalidQueryOnSingleDS() throws Exception
  {
    logger
        .info("PRInvalidQueryJUnitTest#testInvalidQueryOnSingleDS: Test Started  ");
    Region region = PartitionedRegionTestHelper.createPartitionedRegion(
        regionName, "100", 0);

    logger
        .info("PRInvalidQueryJUnitTest#testInvalidQueryOnSingleDS: creating portfolioData objects");
    PortfolioData[] portfolios = new PortfolioData[100];
    for (int j = 0; j < 100; j++) {
      portfolios[j] = new PortfolioData(j);
    }
    populateData(region, portfolios);

    logger
        .info("PRInvalidQueryJUnitTest#testInvalidQueryOnSingleDS: creating Select Query");

    String queryString = "SELECT DISTINCT * FROM /Portfolios WHERE pkid < '5'";

    final String expectedQueryInvalidException = QueryInvalidException.class
        .getName();
    logger.info("<ExpectedException action=add>"
        + expectedQueryInvalidException + "</ExpectedException>");
    try {
      region.query(queryString);
      fail("PRInvalidQueryJUnitTest#testInvalidQueryOnSingleDS: Expected an Invalid Query Exception for the query :"
          + queryString + " this is not supported for region#query()");
    }
    catch (QueryInvalidException qe) {

      logger
          .info("PRInvalidQueryJUnitTest#testInvalidQueryOnSingleDS: Caught an Invalid Query Exception for the query :"
              + queryString + " this is not supported for region#query()");

    }

    logger
        .info("PRInvalidQueryJUnitTest#testInvalidQueryOnSingleDS: Test Ended");

    logger.info("<ExpectedException action=remove>"
        + expectedQueryInvalidException + "</ExpectedException>");
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
        .info("PRInvalidQueryJUnitTest#populateData: Populating Data to the region");

    for (int j = 0; j < data.length; j++) {
      region.put(new Integer(j), data[j]);
    }
  }
}
