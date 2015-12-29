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
package query.common.joinsWithOR;

import hydra.CacheHelper;
import hydra.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.SelectResults;

import query.QueryTest;
import query.common.joinsWithOR.data.BranchCommunityAssignmentImpl;
import query.common.joinsWithOR.data.CommunityImpl;
import query.common.joinsWithOR.data.LoginCommunityAssignmentImpl;
import util.TestException;

/**
 * Hello world!
 * 
 */
public class App {

  static long loginCommunityAssignmentCount = 0;

  static long communityCount = 0;

  static long branchCommunityAssignmentCount = 0;

  static final int COMMUNITY = 0;

  static final int LOGIN_COMMUNITY = 1;

  static final int BRANCH_COMMUNITY = 2;

  static final String queries[] = new String[] {
      "select c.name from /Amoeba/LoginCommunityAssignment as lca, /Amoeba/Community as c "
          + "where lca.communityId = c.id and lca.loginId='lfinker' and "
          + "(c.communityType='CommunityType:FullService' OR c.communityType='CommunityType:Clearing')",
      "select c.name from /Amoeba/LoginCommunityAssignment as lca, "
          + "/Amoeba/Community as c where lca.communityId = c.id and lca.loginId='lfinker' and "
          + "c.communityType='CommunityType:FullService'",
      "select distinct bca.branchId from /Amoeba/LoginCommunityAssignment as lca, "
          + "/Amoeba/Community as c, /Amoeba/BranchCommunityAssignment as bca where lca.loginId='lfinker' "
          + "and lca.communityId = c.id and "
          + "(c.communityType='CommunityType:FullService' or c.communityType='CommunityType:Clearing')" };

  static final String queries_noIndex[] = new String[] {
    "select c.name from /Amoeba/LoginCommunityAssignment_noIndex as lca, /Amoeba/Community_noIndex as c "
        + "where lca.communityId = c.id and lca.loginId='lfinker' and "
        + "(c.communityType='CommunityType:FullService' OR c.communityType='CommunityType:Clearing')",
    "select c.name from /Amoeba/LoginCommunityAssignment_noIndex as lca, "
        + "/Amoeba/Community_noIndex as c where lca.communityId = c.id and lca.loginId='lfinker' and "
        + "c.communityType='CommunityType:FullService'",
    "select distinct bca.branchId from /Amoeba/LoginCommunityAssignment_noIndex as lca, "
        + "/Amoeba/Community_noIndex as c, /Amoeba/BranchCommunityAssignment_noIndex as bca where lca.loginId='lfinker' "
        + "and lca.communityId = c.id and "
        + "(c.communityType='CommunityType:FullService' or c.communityType='CommunityType:Clearing')" };

  public static void HydraTask_queryData() throws Exception {
    Cache cache = CacheHelper.getCache();
    Query query = cache
        .getQueryService()
        .newQuery(
            "select c.name from /Amoeba/LoginCommunityAssignment as lca, /Amoeba/Community as c "
                + "where lca.communityId = c.id and lca.loginId='lfinker' and "
                + "(c.communityType='CommunityType:FullService' OR c.communityType='CommunityType:Clearing')");
    SelectResults result1 = (SelectResults)query.execute();
    Log.getLogWriter().info("OR - result: " + result1);
    System.out.println("OR - result: " + result1);

    // Works without OR
    query = cache
        .getQueryService()
        .newQuery(
            "select c.name from /Amoeba/LoginCommunityAssignment as lca, "
                + "/Amoeba/Community as c where lca.communityId = c.id and lca.loginId='lfinker' and "
                + "c.communityType='CommunityType:FullService'");
    SelectResults result2 = (SelectResults)query.execute();
    System.out.println("Without OR - result: " + result2);
    Log.getLogWriter().info("Without OR - result: " + result2);

    if (result1.size() != result2.size()) {
      throw new TestException("");
    }

    Log.getLogWriter().info("Starting slow query");

    // !!!SLOW!!!
    query = cache
        .getQueryService()
        .newQuery(
            "select distinct bca.branchId from /Amoeba/LoginCommunityAssignment as lca, "
                + "/Amoeba/Community as c, /Amoeba/BranchCommunityAssignment as bca where lca.loginId='lfinker' "
                + "and lca.communityId = c.id and "
                + "(c.communityType='CommunityType:FullService' or c.communityType='CommunityType:Clearing') "
                + "and c.id = bca.communityId");
    long start = System.nanoTime();

    Object result = query.execute();
    long totalTime = System.nanoTime() - start;
    Log.getLogWriter().info(
        "Time taken by slow query::" + (totalTime / 1000) + "ms");
    Log.getLogWriter().info("Result of slow query::" + result);
    System.out.println(result);

  }

  static void executeQueriesAndVerifyResults() throws Exception {
    Object[][] r = new Object[queries.length][2];
    for (int i = 0; i < queries.length; i++) {
      Cache cache = CacheHelper.getCache();
      Query query = cache
          .getQueryService()
          .newQuery(
              queries[i]);
      r[i][0]= (SelectResults)query.execute();
      query = cache.getQueryService().newQuery(queries_noIndex[i]);
      r[i][1] =  (SelectResults)query.execute();
    }
    new QueryTest().compareQueryResultsWithoutAndWithIndexes(r, queries.length, false, queries);
  }

  /**
   * @param cache
   */
  public static void HydraTask_createIndexes() {
    Cache cache = CacheHelper.getCache();
    createIndex(cache.getRegion("/Amoeba/Community"), "id");
    createIndex(cache.getRegion("/Amoeba/LoginCommunityAssignment"), "id");
    createIndex(cache.getRegion("/Amoeba/BranchCommunityAssignment"), "id");

    createIndex(cache.getRegion("/Amoeba/LoginCommunityAssignment"),
        "communityId");
    createIndex(cache.getRegion("/Amoeba/BranchCommunityAssignment"),
        "communityId");

    createIndex(cache.getRegion("/Amoeba/LoginCommunityAssignment"), "loginId");
  }

  private static Index createIndex(Region region, String indexName) {
    try {
      return region
          .getRegionService()
          .getQueryService()
          .createIndex(region.getName() + "." + indexName, indexName,
              region.getFullPath());
    }
    catch (Exception e) {
      throw new RuntimeException(String.format(
          "Failed to create index for '%s' on region '%s'", indexName,
          region.getFullPath()), e);
    }
  }

  public static void HydraTask_countEntries() throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(
        System.getProperty("JTESTS")
            + "/query/generalized/customers/useCase15/data/Community.csv"));
    String line = reader.readLine(); // skip column names
    while ((line = reader.readLine()) != null) {
      communityCount++;
    }

    reader = new BufferedReader(
        new FileReader(
            System.getProperty("JTESTS")
                + "/query/generalized/customers/useCase15/data/LoginCommunityAssignment.csv"));
    line = reader.readLine(); // skip column names
    while ((line = reader.readLine()) != null) {
      loginCommunityAssignmentCount++;
    }

    reader = new BufferedReader(
        new FileReader(
            System.getProperty("JTESTS")
                + "/query/generalized/customers/useCase15/data/BranchCommunityAssignment.csv"));
    line = reader.readLine(); // skip column names
    while ((line = reader.readLine()) != null) {
      branchCommunityAssignmentCount++;
    }
  }

  public static void HydraTask_incrementalPopulateRegions_And_verifyQueries()
      throws Exception {
    // Divide each region into 5 intervals.
    long communityRegionArray[] = new long[6];
    long loginCommunityRegionArray[] = new long[6];
    long branchCommunityRegionArray[] = new long[6];
    communityRegionArray[0] = 0;
    loginCommunityRegionArray[0] = 0;
    branchCommunityRegionArray[0] = 0;
    communityRegionArray[5] = communityCount;
    loginCommunityRegionArray[5] = loginCommunityAssignmentCount;
    branchCommunityRegionArray[5] = branchCommunityAssignmentCount;
    for (int i = 1; i < 5; i++) {
      communityRegionArray[i] = communityRegionArray[i - 1] + communityCount
          / 5;
      loginCommunityRegionArray[i] = loginCommunityRegionArray[i - 1]
          + loginCommunityAssignmentCount / 5;
      branchCommunityRegionArray[i] = branchCommunityRegionArray[i - 1]
          + branchCommunityAssignmentCount / 5;
    }

    for (int i = 0; i < 5; i++) {
      populateRegions(COMMUNITY, communityRegionArray[i],
          communityRegionArray[i + 1], "/Amoeba/Community",
          System.getProperty("JTESTS")
              + "/query/generalized/customers/useCase15/data/Community.csv");
      populateRegions(
          LOGIN_COMMUNITY,
          loginCommunityRegionArray[i],
          loginCommunityRegionArray[i + 1],
          "/Amoeba/LoginCommunityAssignment",
          System.getProperty("JTESTS")
              + "/query/generalized/customers/useCase15/data/LoginCommunityAssignment.csv");
      populateRegions(
          BRANCH_COMMUNITY,
          branchCommunityRegionArray[i],
          branchCommunityRegionArray[i + 1],
          "/Amoeba/BranchCommunityAssignment",
          System.getProperty("JTESTS")
              + "/query/generalized/customers/useCase15/data/BranchCommunityAssignment.csv");
      executeQueriesAndVerifyResults();
    }
  }

  static void populateRegions(int regionId, long startRow, long endRow,
      String regionName, String csvFilePath) throws IOException {
    Cache cache = CacheHelper.getCache();
    Region<Object, Object> region = cache.getRegion(regionName);
    Region<Object, Object> region_noIndex = cache.getRegion(regionName
        + "_noIndex");

    BufferedReader reader = new BufferedReader(new FileReader(csvFilePath));
    String line = reader.readLine(); // skip column names
    int i = 0;
    while (i != startRow && (line = reader.readLine()) != null) {
      i++;
    }
    while ((line = reader.readLine()) != null && i <= endRow) {
      i++;
      String[] split = line.split("\t");

      switch (regionId) {
        case COMMUNITY:
          UUID id = UUID.fromString(split[0]);
          String description = split[1];
          String name = split[2];
          int ordinal = Integer.parseInt(split[3]);
          String ownerId = split[4];
          String communityType = split[5];
          region.put(id, new CommunityImpl(id, name, ownerId, communityType,
              description, ordinal));
          region_noIndex.put(id, new CommunityImpl(id, name, ownerId,
              communityType, description, ordinal));
          break;

        case LOGIN_COMMUNITY:
          id = UUID.fromString(split[0]);
          UUID communityId = UUID.fromString(split[1]);
          String loginId = split[2];
          region.put(id, new LoginCommunityAssignmentImpl(id, communityId,
              loginId));
          region_noIndex.put(id, new LoginCommunityAssignmentImpl(id,
              communityId, loginId));
          break;

        case BRANCH_COMMUNITY:
          id = UUID.fromString(split[0]);
          String branchId = split[1];
          communityId = UUID.fromString(split[2]);
          region.put(id, new BranchCommunityAssignmentImpl(id, branchId,
              communityId));
          region_noIndex.put(id, new BranchCommunityAssignmentImpl(id,
              branchId, communityId));
          break;
        default:
          break;
      }
    }

  }

  /**
   * @param cache
   * @throws IOException
   * 
   */
  public static void HydraTask_importData() throws IOException {
    Cache cache = CacheHelper.getCache();
    Region<Object, Object> communityRegion = cache
        .getRegion("/Amoeba/Community");
    Region<Object, Object> loginCommunityAssignmentRegion = cache
        .getRegion("/Amoeba/LoginCommunityAssignment");
    Region<Object, Object> branchCommunityAssignmentRegion = cache
        .getRegion("/Amoeba/BranchCommunityAssignment");

    communityRegion.clear();
    loginCommunityAssignmentRegion.clear();
    branchCommunityAssignmentRegion.clear();

    BufferedReader reader = new BufferedReader(new FileReader(
        System.getProperty("JTESTS")
            + "/query/generalized/customers/useCase15/data/Community.csv"));
    String line = reader.readLine(); // skip column names
    while ((line = reader.readLine()) != null) {
      String[] split = line.split("\t");
      UUID id = UUID.fromString(split[0]);
      String description = split[1];
      String name = split[2];
      int ordinal = Integer.parseInt(split[3]);
      String ownerId = split[4];
      String communityType = split[5];
      communityRegion.put(id, new CommunityImpl(id, name, ownerId,
          communityType, description, ordinal));
    }

    reader = new BufferedReader(
        new FileReader(
            System.getProperty("JTESTS")
                + "/query/generalized/customers/useCase15/data/LoginCommunityAssignment.csv"));
    line = reader.readLine(); // skip column names
    while ((line = reader.readLine()) != null) {
      String[] split = line.split("\t");
      UUID id = UUID.fromString(split[0]);
      UUID communityId = UUID.fromString(split[1]);
      String loginId = split[2];
      loginCommunityAssignmentRegion.put(id, new LoginCommunityAssignmentImpl(
          id, communityId, loginId));
    }

    reader = new BufferedReader(
        new FileReader(
            System.getProperty("JTESTS")
                + "/query/generalized/customers/useCase15/data/BranchCommunityAssignment.csv"));
    line = reader.readLine(); // skip column names
    while ((line = reader.readLine()) != null) {
      String[] split = line.split("\t");
      UUID id = UUID.fromString(split[0]);
      String branchId = split[1];
      UUID communityId = UUID.fromString(split[2]);
      branchCommunityAssignmentRegion.put(id,
          new BranchCommunityAssignmentImpl(id, branchId, communityId));
    }
  }
}
