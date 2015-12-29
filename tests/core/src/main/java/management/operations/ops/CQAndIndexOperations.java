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
package management.operations.ops;

import static management.util.HydraUtil.logInfo;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import management.operations.MgmtCqListener;
import management.operations.OperationPrms;
import management.operations.RegionKeyValueConfig;
import management.operations.events.CQAndIndexOperationEvents;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqExistsException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryInvalidException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;

@SuppressWarnings("rawtypes")
public class CQAndIndexOperations {

  public static final int CQINDEXOP_CREATE_INDEX = 1;
  public static final int CQINDEXOP_REMOVE_INDEX = 2;
  public static final int CQINDEXOP_CREATE_CQ = 3;
  public static final int CQINDEXOP_STOP_CQ = 4;

  public static final String CQINDEX_CREATE_INDEX = "createIndex";
  public static final String CQINDEX_REMOVE_INDEX = "removeIndex";
  public static final String CQINDEX_CREATE_CQ = "createCq";
  public static final String CQINDEX_STOP_CQ = "stopCq";

  protected static String opPrefix = "CQIndexOperations: ";
  
  protected Region region = null;
  protected RegionKeyValueConfig config = null;
  protected CQAndIndexOperationEvents operationRecorder = null;

  public CQAndIndexOperations(Region region, RegionKeyValueConfig config, CQAndIndexOperationEvents op) {
    this.config = config;
    this.region = region;
    this.operationRecorder = op;
  }

  public void doCqIndexOperation() {
    String opStr = TestConfig.tab().stringAt(OperationPrms.cqIndexOps);

    int op = -1;

    if (CQINDEX_CREATE_INDEX.equals(opStr))
      op = CQINDEXOP_CREATE_INDEX;
    else if (CQINDEX_REMOVE_INDEX.equals(opStr))
      op = CQINDEXOP_REMOVE_INDEX;
    else if (CQINDEX_CREATE_CQ.equals(opStr))
      op = CQINDEXOP_CREATE_CQ;
    else if (CQINDEX_STOP_CQ.equals(opStr))
      op = CQINDEXOP_STOP_CQ;

    switch (op) {
    case CQINDEXOP_CREATE_INDEX:
      _createIndex();
      break;
    case CQINDEXOP_REMOVE_INDEX:
      removeIndex();
      break;
    case CQINDEXOP_CREATE_CQ:
      _createCq();
      break;
    case CQINDEXOP_STOP_CQ:
      stopCq();
      break;
    default:
      logInfo(opPrefix + "Unknown operation code " + op);
      break;
    }
  }

  public String createCq() {
    return _createCq();
  }

  public void removeIndex(String name) {
    QueryService queryService = region.getCache().getQueryService();
    Index index = (Index) queryService.getIndex(region, name);
    if (index == null)
      throw new TestException("Index named " + name + " not found");
    _removeIndex(index);
  }

  public String createIndex() {
    return _createIndex();
  }

  public void stopCq(String name) {
    QueryService queryService = region.getCache().getQueryService();
    CqQuery query = queryService.getCq(name);
    if (query == null)
      throw new TestException("CqQuery named " + name + " not found");
    _stopCq(query);
  }

  private void stopCq() {
    QueryService queryService = region.getCache().getQueryService();
    try {
      CqQuery queries[] = queryService.getCqs(region.getName());
      if (queries != null) {
        int randomNum = TestConfig.tab().getRandGen().nextInt(queries.length - 1);
        CqQuery query = queries[randomNum];
        _stopCq(query);
      } else {
        logInfo("No queries found for region " + region.getName());
      }
    } catch (CqException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private void _stopCq(CqQuery query) {
    try {
      query.stop();
      logInfo(opPrefix + " stopped cq named " + query.getName());
      operationRecorder.cqStopped(query.getName(), query.getQueryString());
    } catch (CqException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private static String[] operatorArray = { "=", "<", ">", ">=", "<=", "<>" };

  private String _createCq() {
    QueryService queryService = region.getCache().getQueryService();
    StringBuilder queryString = new StringBuilder("select * from " + region.getFullPath() + "  where ");
    String column;
    String operator;
    Object randomValue;

    List<String> columnNames = config.getValueColumns();
    column = columnNames.get(TestConfig.tab().getRandGen().nextInt(columnNames.size() - 1));
    operator = operatorArray[TestConfig.tab().getRandGen().nextInt(operatorArray.length - 1)];
    randomValue = TestConfig.tab().getRandGen().nextInt(config.getMaxKeySize());
    queryString.append(column);
    queryString.append(" ").append(operator);
    queryString.append(" ").append(randomValue);

    CqAttributesFactory attributesf = new CqAttributesFactory();
    attributesf.addCqListener(new MgmtCqListener());
    CqAttributes cqAttr = attributesf.create();

    String operatorName = "";
    if ("=".equals(operator))
      operatorName = "EQUALS";
    else if (">".equals(operator))
      operatorName = "GRTHAN";
    else if ("<".equals(operator))
      operatorName = "LESSTHAN";
    else if (">=".equals(operator))
      operatorName = "GRTHANOREQ";
    else if ("<=".equals(operator))
      operatorName = "LESSTHANOREQ";
    else if ("<>".equals(operator))
      operatorName = "NOTEQUALS";

    String name = "cq_" + region.getName() + "_" + column + "_" + operatorName + "_" + randomValue;
    try {
      queryService.newCq(name, queryString.toString(), cqAttr);
      logInfo(opPrefix + " created cq named " + name + " with string <" + queryString.toString() + ">");
      operationRecorder.cqCreated(name, queryString.toString(), null);
      return name;
    } catch (QueryInvalidException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (CqExistsException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    } catch (CqException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }

  }

  private void removeIndex() {
    QueryService queryService = region.getCache().getQueryService();
    Collection<Index> indexes = queryService.getIndexes(region);
    if (indexes != null && indexes.size() > 0) {
      int randomNum = TestConfig.tab().getRandGen().nextInt(0, (indexes.size() - 1));
      Index index = (Index) indexes.toArray()[randomNum];
      _removeIndex(index);
    } else {
      logInfo("No indexes found for region " + region.getName());
    }
  }

  private void _removeIndex(Index index) {
    QueryService queryService = region.getCache().getQueryService();
    try {
      queryService.removeIndex(index);
      logInfo(opPrefix + " removed index named " + index.getName());
      operationRecorder.indexRemoved(index.getName());
    } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  private String _createIndex() {
    QueryService queryService = region.getCache().getQueryService();
    Collection<Index> indexes = queryService.getIndexes(region);

    if (indexes == null)
      indexes = new ArrayList<Index>();

    List<String> columnNames = config.getValueColumns();
    List<String> indexList = new ArrayList<String>();

    for (String s : columnNames) {
      boolean flagFound = false;
      for (Index x : indexes) {
        if (x.getName().equals(("indexForColumn" + s + "ForRegion" + region.getName()))) {
          flagFound = true;
        }
      }
      if (!flagFound)
        indexList.add(s);
    }
    if (indexList.size() != 0) {
      int randomNum = TestConfig.tab().getRandGen().nextInt(0, (indexList.size() - 1));
      String selected = indexList.get(randomNum);
      String indexName = "indexForColumn" + selected + "ForRegion" + region.getName();

      try {
        queryService.createIndex(indexName, selected, region.getFullPath());
        logInfo(opPrefix + " created index named " + indexName + " on column " + selected + " for region "
            + region.getName());
        operationRecorder.indexCreated(indexName, selected, region.getFullPath());
        return indexName;
      } catch (RegionNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IndexInvalidException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      } catch (IndexNameConflictException e) {
        // ignore and continue
        logInfo("Index named indexForColumn" + selected + " already exists");
      } catch (IndexExistsException e) {
        // e.printStackTrace();
        logInfo("Index named indexForColumn" + selected + " already exists");
      } catch (UnsupportedOperationException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
    } else {
      logInfo("All properties have been used up. New index can not be created.. skipping the iteration.");
      return null;
    }
    return null;
  }

  protected String fromClause() {
    return this.region.getFullPath();
  }

}
