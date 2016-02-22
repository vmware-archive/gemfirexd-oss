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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.util.HashSet;
import java.util.Set;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireActivation;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;
import io.snappydata.test.dunit.DistributedTestBase;
import org.apache.log4j.Logger;

/**
 * Helper class used  to calculate pruned nodes 
 * and comparing with the nodes computes by
 * SelectQueryInfo or UpdateQueryInfo
 * @author Asif
 *
 */
public class NodesPruningHelper {

  public final static int noCheck = 0, byrange = 1, bylist = 2, bycolumn = 3, ANDing = 4, ORing = 5, allnodes = 6;
//////////////////////////////////////////////////////////////////////////////////
  public static void setupObserverOnClient(final QueryInfo[] sqi) {
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    // final SelectQueryInfo[] sqi = _qi;
    // set up a sql query observer in client VM
    GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter() {
          @Override
          public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qi,GenericPreparedStatement gps, LanguageConnectionContext lcc) {
            sqi[0] = qi;
          }
        });

  }

  public static void setupObserverOnClient(final QueryInfo[] sqi, final Activation[] activationArray) {
    // Create a prepared statement for the query & store the queryinfo so
    // obtained to get
    // region reference
    // final SelectQueryInfo[] sqi = _qi;
    // set up a sql query observer in client VM
    GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter() {
          @Override
          public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qi, GenericPreparedStatement gps, LanguageConnectionContext lcc) {
            sqi[0] = qi;
          }

          @Override
          public void beforeGemFireResultSetExecuteOnActivation(
              AbstractGemFireActivation activation) {
            activationArray[0] = activation;
          }
        });
  }

  public static void validateNodePruningForQuery(final String query,
      QueryInfo sqi, Object[][] routingInfo, DistributedTestBase test,
      Activation activation) throws StandardException {

    Set<DistributedMember> expected = getExpectedNodes(query,
        sqi, routingInfo, test.getLogWriter());
    Set<DistributedMember> actual = new HashSet<DistributedMember>();
    Set<Object> actualRoutingKeys = new HashSet<Object>();
    actualRoutingKeys.add(ResolverUtils.TOK_ALL_NODES);
    sqi.computeNodes(actualRoutingKeys, activation, false);
    actual.addAll(convertRoutingKeysIntoMembersForPR(actualRoutingKeys,
        (PartitionedRegion)sqi.getRegion()));
    test.assertEquals(expected.size(), actual.size());
    actual.removeAll(expected);
    test.assertTrue(actual.isEmpty());
  }

  public static void validateNodePruningForQuery(final String query,
      QueryInfo sqi, Object[][] routingInfo,
      DistributedTestBase test) throws StandardException {
    validateNodePruningForQuery(query, sqi, routingInfo, test, null);
  }

  public static Set<InternalDistributedMember> convertRoutingKeysIntoMembersForPR(
      Set<Object> routingKeys, PartitionedRegion prgn) {
    if (routingKeys.contains(ResolverUtils.TOK_ALL_NODES)) {

      Set<InternalDistributedMember> nodesOfPr = prgn
          .getRegionAdvisor().adviseDataStore();
      if (prgn.getLocalMaxMemory() > 0) {
        nodesOfPr.add(prgn.getDistributionManager().getId());
      }
      return nodesOfPr; 
    }
    else {
      Set<InternalDistributedMember> nodesOfPr = prgn
          .getMembersFromRoutingObjects(routingKeys.toArray());
      return nodesOfPr;

    }
  }
 
  /**
   * 
   * @param query
   * @param sqi
   * @param routingInfo . It is two dimensional Object[][]. Each row represents the pruning  which
   * can be accomplished by a condition. Two consecutive rows (conditions) are tied by an AND or OR junction  
   * @return
   * @throws StandardException
   */
  public static Set<DistributedMember> getExpectedNodes(final String query,
      QueryInfo sqi, Object[][] routingInfo,
      Logger logger) throws StandardException {
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    pr.getPartitionResolver();
    Set<InternalDistributedMember> allNodes = pr
        .getRegionAdvisor().adviseDataStore();
    Set<DistributedMember> expected = new HashSet<DistributedMember>();
    expected.addAll(allNodes);
    for (int i = 0; i < routingInfo.length; ++i) {
      Object[] currentConditionRow = routingInfo[i];
      int partitioningType = ((Integer)currentConditionRow[0]).intValue();
      switch (partitioningType) {
        case byrange:
          handleRangePartitioning(sqi, currentConditionRow, allNodes,
              expected, i, logger);
          break;
        case bylist:
          handleListPartitioning(sqi, currentConditionRow, allNodes,
              expected, i, logger);
          break;
        case bycolumn:
          handleColumnPartitioning(sqi, currentConditionRow, allNodes,
              expected, i, logger);
          break;
        case allnodes:
          expected.addAll(allNodes);
          break;
        default:
          throw new IllegalArgumentException("The partition type is undefined");
      }
    }
    return expected;

  }

  private static void handleRangePartitioning(QueryInfo sqi,
      Object[] currentRowRoutingInfo, Set<InternalDistributedMember> allNodes,
      Set<DistributedMember> prunedNodes, int conditionNumIndx, Logger logger) {
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    //Identify the nodes which should see the query based on partition resolver
    GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();   
    Set<DistributedMember> currentCondnNodes = null;  
      //Each row represents a condition
      
      currentCondnNodes = null;
      DataValueDescriptor lowerbound = (DataValueDescriptor)currentRowRoutingInfo[1];
      DataValueDescriptor upperbound = (DataValueDescriptor)currentRowRoutingInfo[3];
      boolean lboundinclusive = false , uboundinclusive= false ;
       if(lowerbound != null) {
          lboundinclusive = ((Boolean)currentRowRoutingInfo[2]).booleanValue();
       }   
      
      if(upperbound != null) {
          uboundinclusive = ((Boolean)currentRowRoutingInfo[4]).booleanValue();
       }   
     
      Object[] routingObjects = spr.getRoutingObjectsForRange(lowerbound, lboundinclusive, upperbound, uboundinclusive);
      routingObjects= routingObjects!=null? routingObjects: new Object[]{};
        
       
      logger.info("Range   ["+lowerbound+" inclusive = "+ lboundinclusive + " to "+upperbound+ " inclusive = "+uboundinclusive+ "] : routing keys = "+routingObjects.length);

      for(Object entry : routingObjects) {
        logger.info("   routing keys = [" + entry.toString() + "]");
      }
      
      if(routingObjects.length > 0) {
        currentCondnNodes = pr.getMembersFromRoutingObjects(routingObjects);        
      }
      
      if( currentRowRoutingInfo.length > 5 ) {
        //Look for the junction Op
        if(((Integer)currentRowRoutingInfo[5]).intValue() == ANDing)  {
          if(currentCondnNodes != null) {
            prunedNodes.retainAll(currentCondnNodes);
            
          }        
          
        }else {
          //OR junction
          if(currentCondnNodes != null) {
            prunedNodes.addAll(currentCondnNodes);            
          }else {
            prunedNodes.addAll(allNodes);
          }            
        }
        
      }else {
        //case of starting row
        if(currentCondnNodes != null) {
          prunedNodes.retainAll(currentCondnNodes);
        }      
      }   
    
  }

  private static void handleColumnPartitioning(QueryInfo sqi,
      Object[] currentRowRoutingInfo, Set<InternalDistributedMember> allNodes,
      Set<DistributedMember> prunedNodes, int conditionNumIndx, Logger logger) {
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    //Identify the nodes which should see the query based on partition resolver
    GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();   
    Set<DistributedMember> currentCondnNodes = null;  
      //Each row represents a condition      
    currentCondnNodes = null;
    int end = (conditionNumIndx ==0) ?currentRowRoutingInfo.length:currentRowRoutingInfo.length-1;
    DataValueDescriptor[] keysForIn = new DataValueDescriptor[end-1];
    for(int i = 1; i <end ;++i) {
       keysForIn[i-1] = (DataValueDescriptor)currentRowRoutingInfo[i];
    }

      for(Object entry : keysForIn) {
        logger.info(" Column based partitioning: partition key = [" + entry.toString() + "]");
      }
      Object routingKey = null;
      if(spr == null) {
        int hash = 0;
        for(int i = 0; i <keysForIn.length ;++i) {          
            hash^= keysForIn[i].hashCode();          
        }
        routingKey = Integer.valueOf(hash);
        
      }else {
        routingKey = spr.getRoutingObjectsForPartitioningColumns(keysForIn);
      }
      Object[] routingObjects= routingKey!=null?new Object[]{routingKey}: new Object[]{};
      for(Object entry : routingObjects) {
        logger.info(" Column based partition key Routing key= [" + entry.toString() + "]");
      }
      
      if(routingObjects.length > 0) {
        currentCondnNodes = pr.getMembersFromRoutingObjects(routingObjects);        
      }
      
      if( conditionNumIndx > 0) {
        //Look for the junction Op
        if(((Integer)currentRowRoutingInfo[currentRowRoutingInfo.length -1]).intValue() == ANDing)  {
          if(currentCondnNodes != null) {
            prunedNodes.retainAll(currentCondnNodes);
            
          }        
          
        }else {
          //OR junction
          if(currentCondnNodes != null) {
            prunedNodes.addAll(currentCondnNodes);            
          }else {
            prunedNodes.addAll(allNodes);
          }            
        }        
      }else {
        //case of starting row
        if(currentCondnNodes != null) {
          prunedNodes.retainAll(currentCondnNodes);
        }      
      }    
  }

  private static void handleListPartitioning(QueryInfo sqi,
      Object[] currentRowRoutingInfo, Set<InternalDistributedMember> allNodes,
      Set<DistributedMember> prunedNodes, int conditionNumIndx, Logger logger) {
    PartitionedRegion pr = (PartitionedRegion)sqi.getRegion();
    //Identify the nodes which should see the query based on partition resolver
    GfxdPartitionResolver spr = (GfxdPartitionResolver)pr.getPartitionResolver();   
    Set<DistributedMember> currentCondnNodes = null;  
      //Each row represents a condition      
      currentCondnNodes = null;
      int end = (conditionNumIndx ==0) ?currentRowRoutingInfo.length:currentRowRoutingInfo.length-1;
      DataValueDescriptor[] keysForIn = new DataValueDescriptor[end-1];
      for(int i = 1; i <end ;++i) {
        keysForIn[i-1] = (DataValueDescriptor)currentRowRoutingInfo[i];
      }

      for(Object entry : keysForIn) {
        logger.info(" List partition key of In= [" + entry.toString() + "]");
      }
      
      Object[] routingObjects = spr.getRoutingObjectsForList(keysForIn);
      routingObjects= routingObjects!=null? routingObjects: new Object[]{};
      for(Object entry : routingObjects) {
        logger.info(" List partition key Routing key= [" + entry.toString() + "]");
      }
      
      if(routingObjects.length > 0) {
        currentCondnNodes = pr.getMembersFromRoutingObjects(routingObjects);        
      }
      
      if( conditionNumIndx > 0) {
        //Look for the junction Op
        if(((Integer)currentRowRoutingInfo[currentRowRoutingInfo.length -1]).intValue() == ANDing)  {
          if(currentCondnNodes != null) {
            prunedNodes.retainAll(currentCondnNodes);
            
          }        
          
        }else {
          //OR junction
          if(currentCondnNodes != null) {
            prunedNodes.addAll(currentCondnNodes);            
          }else {
            prunedNodes.addAll(allNodes);
          }            
        }
        
      }else {
        //case of starting row
        if(currentCondnNodes != null) {
          prunedNodes.retainAll(currentCondnNodes);
        }      
      }    
  } 
  

}
