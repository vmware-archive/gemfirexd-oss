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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.AbstractRegion;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;

/**
 * The interface representing the analysis of the optimized query tree. It gives
 * information regarding the number of nodes on which this query needs to be
 * sent or whether it can be converted into a Region.get() etc.
 * 
 * @author Asif
 * @since GemFireXD
 * 
 */
public interface QueryInfo extends QueryInfoConstants, Serializable {
  public static final byte HAS_ORDERBY = 0x01;
  public static final byte HAS_GROUPBY = 0x02; 
  public static final byte IS_PRIMARY_KEY_TYPE = 0x04;
  public static final byte HAS_DISTINCT = 0x08;
  public static final byte HAS_DISTINCT_SCAN = 0x10;  
  public static final byte EMBED_GFE_RESULTSET = 0x20;
  public static final byte HAS_PR_DEPENDENT_SUBQUERY = 0x40;
  public static final short HAS_FETCH_ROWS_ONLY = 0x80;
  public static final short DISALLOW_SUBQUERY_FLATTENING = 0x100;
  public static final short SELECT_FOR_UPDATE = 0x200;  
  public static final short IS_SUBQUERY = 0x400;
  public static final short HAS_JOIN_NODE = 0x800;
  public static final short IS_GETALL_ON_LOCAL_INDEX = 0x1000;
  // Independent of @see JunctionQueryInfo.IS_STATIC_NOT_CONVERTIBLE
  public static final short MARK_NOT_GET_CONVERTIBLE = 0x2000;
  public static final short NCJ_LEVEL_TWO_QUERY_HAS_VAR_IN = 0x4000;
  
  /**
   * 
   * @return true if the query can be converted into a Region.get or Region.put for
   * the respective select & update statement
   */
  public boolean isPrimaryKeyBased() throws StandardException;

  /**
   * 
   * @return Object representing the primary key for the table
   */
  public Object getPrimaryKey() throws StandardException;
  
  public Object getIndexKey() throws StandardException;
  
  /**
   * 
   * @param activation Activation object . This will be utilized in PreparedStatement
   * queries to obtain parameter values for node computation
   * @param forSingleHopPreparePhase TODO
   * @param nodes Set containing DistributedMember objects on which the query 
   * needs to be distributed to
   * @exception StandardException
   */
  public void computeNodes(Set<Object> routingKeys, Activation activation,
      boolean forSingleHopPreparePhase) throws StandardException;
  //public void computeNodes( Set<DistributedMember> nodes) throws StandardException;

  /**
   * 
   * @return the underlying Gemfire Region for the table . This should ideally
   *         be invoked only if the isConvertibleToGet is true. In case of
   *         multiple tables being present ( implying isConvertibleToGet is
   *         false) it will currently return the Region handle for the first
   *         table of the query.
   * 
   */
  public AbstractRegion getRegion();

  /**
   * 
   * @return true if the statement is for a Select Query. At some point it may
   *         make sense for it to return an int indicating if the statement is a
   *         Select/Insert/Create/Drop etc
   */
  public boolean isSelect();
  
  /**
   * 
   * @return true if the statement is for an Update type Query. At some point it may
   *         make sense for it to return an int indicating if the statement is a
   *         Select/Insert/Create/Drop etc
   */
  public boolean isUpdate();

  /**
   * 
   * @return true if the statement is for a delete type Query. At some point it may
   *         make sense for it to return an int indicating if the statement is a
   *         Select/Insert/Create/Drop etc
   */
  public boolean isDelete();
  
  /** 
   * This method determines where various aspects of a query is present or not
   * for instance where clause, orderby clause or group by is there in a query or not.
   * 
   * @return true if the Query is what flag(s) asks for.
   */
  public boolean isQuery(int flags);

  /**
   * same as above but can take multiple flags
   *
   * @return true if the Query is what flag(s) asks for.
   */
  public boolean isQuery(int ... flags);
  /**
   * This is to return individual QueryInfo's querytype set and to be merged to 
   * SelectQueryInfo object instance.
   * 
   * @return queryType bit flags of the inherited QI child classes.
   
  public int getQueryType();*/
  
  /** This function returns order by column ordering as per projection of the query.
   * Projection column may be derived expression and individual columns might be in
   * ascending or decending order. 
   * 
   * @see ColumnOrdering
   * @return Column ordering info array.
   
  public ColumnOrdering[] getColumnOrdering();*/

  public boolean isSelectForUpdateQuery();
  
  public boolean needKeysForSelectForUpdate();

  public String getTableName();

  public String getFullTableName();

  public String getSchemaName();

  /**
   * 
   * @return ResultDescription object which is used to generate
   *         ResultSetMetaData object
   */
  public ResultDescription getResultDescription();

  /**
   * 
   * @return true if the query as a whole is not statically evaluatable i.e either the
   *         where clause is parameterized  or the update columns fields are
   *         parameterized, which will usually be the case with
   *         PreparedStatement, false otherwise
   */
  // TODO:Asif: This method is currently meaningfully implemented
  // only for SelectQueryInfo. Should we meanigfully implement for individual
  // QueryInfo objects also? That is the subparts of the Query for eg
  // A comparison Query info or , Junction QueryInfo? It might be needed
  // / later
  public boolean isDynamic();
  
  
  /**
   * 
   * @return int indicating the number of parameters if any, in the query
   */
  public int getParameterCount();
  
  /**
   * @return true if statement is an insert statement.
   */
  public boolean isInsert();

  public boolean createGFEActivation() throws StandardException;

  public boolean isOuterJoin();

  public List<Region> getOuterJoinRegions();

  public void setOuterJoinSpecialCase();

  public boolean isOuterJoinSpecialCase();

  public boolean isTableVTI();

  public boolean routeQueryToAllNodes();

  public boolean isDML();

  public void setInsertAsSubSelect(boolean b, String targetTable);

  public boolean isInsertAsSubSelect();
  
  public String getTargetTableName();
  
  public boolean hasUnionNode();
  
  public boolean hasIntersectOrExceptNode();

  public void throwExceptionForInvalidParameterizedData(int validateResult) throws StandardException;
  
  public boolean hasSubSelect();
}
