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
package objects.query.broker;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;

import hydra.BasePrms;
import hydra.Log;
import hydra.RegionHelper;

import java.util.ArrayList;
import java.util.Map;
import java.util.List;

import objects.query.BaseOQLQueryFactory;
import objects.query.QueryObjectException;
import objects.query.QueryPrms;

//----------------------------------------------------------------------------
// OQLQueryFactory
//----------------------------------------------------------------------------

public class OQLBrokerQueryFactory extends BaseOQLQueryFactory  {

  private static Region BrokerRegion;

  private OQLBrokerTicketQueryFactory brokerTicketQueryFactory;

  private static synchronized void setRegion(Region r) {
    BrokerRegion = r;
  }

  public OQLBrokerQueryFactory() {
    brokerTicketQueryFactory = new OQLBrokerTicketQueryFactory();
  }

  public void init() {
    super.init();
    brokerTicketQueryFactory.init();
  }

  //--------------------------------------------------------------------------
  // QueryFactory : regions
  //--------------------------------------------------------------------------

  public void createRegions() {
    setRegion(RegionHelper.createRegion(Broker.getTableName(),
                           BrokerPrms.getBrokerRegionConfig()));
    brokerTicketQueryFactory.createRegions();
  }
  
  //--------------------------------------------------------------------------
  // QueryFactory : indices
  //--------------------------------------------------------------------------
  public void createIndexes() {
    //DO SOMETHING
   
  }
  
  //--------------------------------------------------------------------------
  // QueryFactory : constraints
  //--------------------------------------------------------------------------
  public List getConstraintStatements() {
    //DO SOMETHING
    return new ArrayList();
  }
  
  //--------------------------------------------------------------------------
  // QueryFactory : inserts
  //--------------------------------------------------------------------------

  /**
   * Generates the list of insert objects required to create a broker with the
   * given broker id.
   *
   * @param bid the unique broker id
   * @throw QueryObjectException if bid exceeds {@link BrokerPrms#numBrokers}.
   */
  public List getInsertObjects(int bid) {
    int numBrokers = BrokerPrms.getNumBrokers();
    if (bid >= numBrokers) {
      String s = "Attempt to get insert object with bid=" + bid + " when "
          + BasePrms.nameForKey(BrokerPrms.numBrokers) + "=" + numBrokers;
      throw new QueryObjectException(s);
    }
    List objs = new ArrayList();
    Broker obj = new Broker();
    obj.init(bid);
    objs.add(obj);
    objs.add(brokerTicketQueryFactory.getInsertObjects(bid));
    return objs;
  }

  public List getPreparedInsertObjects() {
    List pobjs = new ArrayList();
    Object pobj = new Broker();
    pobjs.add(pobj);
    pobjs.add(brokerTicketQueryFactory.getPreparedInsertObjects());
    return pobjs;
  }

  // @todo Logging (either move it or implement it here)
  // @todo implement returning map
  /**
   * @throw QueryObjectException if bid has already been inserted.
   */
  public Map fillAndExecutePreparedInsertObjects(List pobjs, int i) {
    Broker pobj = (Broker)pobjs.get(0);
    pobj.init(i);
    if (logUpdates) {
      Log.getLogWriter().info("Executing update: " + pobj);
    }
    // @todo use ObjectHelper to generate configurable key types
    BrokerRegion.put(String.valueOf(pobj.getId()), pobj);
    if (logUpdates) {
      Log.getLogWriter().info("Executed update: " + pobj);
    }
    brokerTicketQueryFactory.fillAndExecutePreparedInsertObjects((List)pobjs.get(1), pobj.getId());
    return null;
  }

//==============================================================================
// HERE

  public OQLIndexInfo getPrimaryKeyIndexOnBrokerId() {
    return new OQLIndexInfo("brokerId", IndexType.PRIMARY_KEY, "id", "/"
        + Broker.REGION_TABLE_NAME);
  }

  public OQLIndexInfo getFunctionalIndexOnBrokerId(String indexType) {
    return new OQLIndexInfo("brokerId", IndexType.FUNCTIONAL, "id", "/"
        + Broker.REGION_TABLE_NAME);
  }

  public OQLIndexInfo getFunctionalIndexOnBrokerName() {
    return new OQLIndexInfo("brokerName", IndexType.FUNCTIONAL, "name", "/"
        + Broker.REGION_TABLE_NAME);
  }

  public int getQueryType() {
    return BrokerPrms.getQueryType(QueryPrms.OQL);
  }
  
  public int getUpdateQueryType() {
    return BrokerPrms.getUpdateQueryType(QueryPrms.OQL);
  }
  public int getDeleteQueryType() {
    return BrokerPrms.getDeleteQueryType(QueryPrms.OQL);
  }

  public String getQuery(int queryType, int i) {
    //do something
    return "";
  }
  
  public String getPreparedQuery(int queryType) {
    //do something
    return "";
  }

  public String getRandomEqualityOnBrokerIdQuery(int i) {
    /*
    String brokerRegionName = BrokerPrms.getBrokerRegionName();
    Region brokerRegion = RegionHelper.getRegion(brokerRegionName);

    int numBrokers = BrokerPrms.getNumBrokers();
    int resultSetSize = BrokerPrms.getResultSetSize();
    if (resultSetSize != 1) {
      String s = "Illegal result set size for equality query on Broker.id: "
               + " resultSetSize + ", must be 1";
      throw new QueryObjectException(s);
    }
    int brokerId = rng.nextInt(0, numBrokers - 1);

    String query = "select distinct * from " + region.getFullPath()
                 + " where id" + " = " + "'" + String.valueOf(brokerId) + "'";
    if (logQueries) {
      Log.getLogWriter().info("Generated query: " + query);
    }
    return query;
    */
    return "";
  }

  class OQLIndexInfo {
    private IndexType indexType;
    private String name;
    private String indexedExpression;
    private String fromClause;

    public OQLIndexInfo(String name, IndexType indexType,
        String indexedExpression, String fromClause) {
      this.name = name;
      this.indexType = indexType;
      this.indexedExpression = indexedExpression;
      this.fromClause = fromClause;
    }
  }

  //----------------------------------------------------------------------------
  //OQL Stuff
  //----------------------------------------------------------------------------

  /**
   * Gets the random range query on the "id" field.
   */
  /**
   * Gets the random range query on the "price" field.
   */
  /**
   * Gets the random equality query on the "ticker" field.
   */
  /**
   * Gets the random join query on the "ticker" and "brokerId" fields
   * with the secondary region object "id" field.
   */

//------------------------------------------------------------------------------
// ConfigurableObject
//------------------------------------------------------------------------------
/*
  public void init() {
    todo
  }

  public int getIndex() {
    return this.id;
  }

  public void validate(int index) {
    int encodedIndex = this.getIndex();
    if (encodedIndex != index) {
      String s = "Expected index " + index + ", got " + encodedIndex;
      throw new ObjectValidationException(s);
    }
  }

  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    } else if (obj instanceof Broker) {
      Broker broker = (Broker)obj;
      return this.id == broker.id;
    } else {
      return false;
    }
  }

  public int hashCode() {
    return this.id;
  }
*/
//----------------------------------------------------------------------------
//OQLQueryFactory
//----------------------------------------------------------------------------
  /**
   * Determines how to read the OQL results from the result object given the querytype
   * @param resultSet
   */
  public void readResultSet(int queryType, Object resultSet) {
    
  }
  
  /**
   * Determines how to fill and execute a given query and queryType
   */
  public Object fillAndExecutePreparedQueryStatement(Query query,
      int queryType, int id) {
    // TODO Auto-generated method stub
    return null;
  }

}
