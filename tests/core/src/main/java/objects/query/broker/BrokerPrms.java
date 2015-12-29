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

import hydra.BasePrms;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.RegionPrms;

import java.util.Vector;

import objects.query.QueryPrms;

/**
 * A class used to store keys for test configuration settings.
 */
public class BrokerPrms extends BasePrms {

  public static final String RANDOM_EQUALITY_ON_BROKER_ID = "randomEqualityOnBrokerId";
  public static final String EQUALITY_ON_BROKER_ID = "equalityOnBrokerId";
  public static final String TICKETS_FROM_EQUALITY_ON_BROKER_ID = "ticketsFromEqualityOnBrokerId";
  public static final String TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE = "ticketsFromRandomRangeOnTicketPrice";
  public static final String BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE = "brokersFromRandomRangeOnTicketPrice";
  public static final String BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE = "brokersFromRandomSizeRangeOnTicketPrice";
  public static final String BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE = "brokersFromRandomPercentageRangeOnTicketPrice";
  
  protected static final int RANDOM_EQUALITY_ON_BROKER_ID_QUERY = 0;
  protected static final int EQUALITY_ON_BROKER_ID_QUERY = 1;
  protected static final int TICKETS_FROM_EQUALITY_ON_BROKER_ID_QUERY = 2;
  protected static final int TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY = 3;
  protected static final int BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY = 4;
  protected static final int BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE_QUERY = 5;
  protected static final int BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE_QUERY = 6;

  protected static final String PRIMARY_KEY_INDEX_ON_BROKER_ID = "primaryKeyIndexOnBrokerId";
  protected static final String UNIQUE_INDEX_ON_BROKER_NAME = "uniqueKeyIndexOnBrokerName";
  
  protected static final String PRIMARY_KEY_INDEX_ON_BROKER_TICKET_ID = "uniqueKeyIndexOnBrokerTicketId";
  protected static final String UNIQUE_INDEX_ON_BROKER_TICKET_NAME = "uniqueKeyIndexOnBrokerTicketName";

  protected static final int PRIMARY_KEY_INDEX_ON_BROKER_ID_QUERY = 0;
  protected static final int UNIQUE_INDEX_ON_BROKER_NAME_QUERY = 1;

  protected static final int PRIMARY_KEY_INDEX_ON_BROKER_TICKET_ID_QUERY = 2;
  protected static final int UNIQUE_INDEX_ON_BROKER_TICKET_NAME_QUERY = 3;
 
  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long brokerDataPolicy;
  public static int getBrokerDataPolicy() {
    Long key = brokerDataPolicy;
    String val = tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }
  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long brokerTicketDataPolicy;
  public static int getBrokerTicketDataPolicy() {
    Long key = brokerTicketDataPolicy;
    String val = tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }
  
  /**
   * (String(s))
   * A Vector of index types to create
   */
  public static Long indexTypes;
  public static Vector getIndexTypes() {
    Long key = indexTypes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));    
    return val;
  } 
 
  public static int getIndexType(String val) {
    Long key = indexTypes;
    if (val.equals(PRIMARY_KEY_INDEX_ON_BROKER_ID)) {
      return PRIMARY_KEY_INDEX_ON_BROKER_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_BROKER_NAME)) {
      return UNIQUE_INDEX_ON_BROKER_NAME_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_BROKER_TICKET_ID)) {
      return PRIMARY_KEY_INDEX_ON_BROKER_TICKET_ID_QUERY;
    } 
    else if (val.equals(UNIQUE_INDEX_ON_BROKER_TICKET_NAME)) {
      return UNIQUE_INDEX_ON_BROKER_TICKET_NAME_QUERY;
    }
    else {
      String s = "Unsupported value for "
               //+ QueryPrms.getAPIString(QueryPrms.SQL) + " index "
               + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }
  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long queryType;
  public static int getQueryType(int api) {
    Long key = queryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api)
    {
      case QueryPrms.OQL:
        return getOQLQueryType(key, val);
      case QueryPrms.GFXD:
        return getSQLQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }
  
  private static int getOQLQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(RANDOM_EQUALITY_ON_BROKER_ID)) {
      return RANDOM_EQUALITY_ON_BROKER_ID_QUERY;
    }
    else {
      String s = "Unsupported value for "
               + QueryPrms.getAPIString(QueryPrms.OQL) + " query "
               + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }
  private static int getSQLQueryType(Long key, String val) {
    if (val.equals(RANDOM_EQUALITY_ON_BROKER_ID)) {
      return RANDOM_EQUALITY_ON_BROKER_ID_QUERY;
    }
    else if (val.equals(EQUALITY_ON_BROKER_ID)) {
      return EQUALITY_ON_BROKER_ID_QUERY;
    }
    else if (val.equals(TICKETS_FROM_EQUALITY_ON_BROKER_ID)) {
      return TICKETS_FROM_EQUALITY_ON_BROKER_ID_QUERY;
    }
    else if (val.equals(TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE)) {
      return TICKETS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY;
    }
    else if (val.equals(BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE)) {
      return BROKERS_FROM_RANDOM_RANGE_ON_TICKET_PRICE_QUERY;
    }
    else if (val
        .equals(BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE)) {
      return BROKERS_FROM_RANDOM_SIZE_RANGE_ON_TICKET_PRICE_QUERY;
    }
    else if (val
        .equals(BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE)) {
      return BROKERS_FROM_RANDOM_PERCENTAGE_RANGE_ON_TICKET_PRICE_QUERY;
    }
    else {
      String s = "Unsupported value for "
               + "SQL query "
               + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }
  
  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long updateQueryType;
  public static int getUpdateQueryType(int api) {
    Long key = updateQueryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api)
    {
      case QueryPrms.OQL:
        return getSQLUpdateQueryType(key, val);
      case QueryPrms.GFXD:
        return getSQLUpdateQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }
  
  private static int getOQLUpdateQueryType(Long key, String val) {
      String s = "Unsupported value for "
               + QueryPrms.getAPIString(QueryPrms.OQL) + " query "
               + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
  }
  private static int getSQLUpdateQueryType(Long key, String val) {
   
      String s = "Unsupported value for "
               + "SQL query "
               + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
  }

  /**
   * (String(s))
   * Type of query.
   */
  public static Long deleteQueryType;
  public static int getDeleteQueryType(int api) {
    Long key = deleteQueryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api)
    {
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }
  
  /**
   * (String(s))
   * Fields to return in result set.  Defaults to "*".
   */
  public static Long brokerFields;
  public static String getBrokerFields() {
    Long key = brokerFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    //todo
    return Broker.commaSeparatedStringFor(val);
  }

  /**
   * (String(s))
   * Fields to return in result set.  Defaults to "*".
   */
  public static Long brokerTicketFields;
  public static String getBrokerTicketFields() {
    Long key = brokerTicketFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return BrokerTicket.commaSeparatedStringFor(val);
  }
  
  public static String getFields() {
    String fields = "";
    if (BrokerPrms.getBrokerTicketFields().indexOf("NONE") != -1) {
      if (BrokerPrms.getBrokerFields().indexOf("NONE") != -1 ) {
        //both can't be none
        String s = BasePrms.nameForKey(brokerFields) + " and " 
          + BasePrms.nameForKey(brokerTicketFields) + " cannot both be NONE";
        throw new HydraConfigException(s);
      }
      else {
        fields = BrokerPrms.getBrokerFields();
      }
    }
    else if (BrokerPrms.getBrokerFields().indexOf("NONE") != -1) {
      fields = BrokerPrms.getBrokerTicketFields();
    }
    else {
     fields =
        BrokerPrms.getBrokerFields() + "," + BrokerPrms.getBrokerTicketFields();
    }
    if (fields.indexOf("*") != -1) {
      fields = "*";
    }
    return fields;
  }

  /**
   * (boolean)
   * Whether to return distinct results.  Defaults to false.
   */
  public static Long distinct;
  public static boolean getDistinct() {
    Long key = distinct;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (boolean)
   * Whether to use best fit in computing the result set.  Defaults to false.
   */
  public static Long useBestFit;
  public static boolean useBestFit() {
    Long key = useBestFit;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  /**
   * (String)
   * Name of broker region configuration from {@link hydra.RegionPrms#names}.
   * The region name is {@link Broker#REGION_TABLE_NAME}.
   */
  public static Long brokerRegionConfig;
  public static String getBrokerRegionConfig() {
    Long key = brokerRegionConfig;
    return tab().stringAt(key);
  }

  /**
   * (String)
   * Name of broker ticket region configuration from {@link hydra.RegionPrms
   * #names}.  The region name is {@link BrokerTicket#REGION_TABLE_NAME}.
   */
  public static Long brokerTicketRegionConfig;
  public static String getBrokerTicketRegionConfig() {
    Long key = brokerTicketRegionConfig;
    return tab().stringAt(key);
  }

  /**
   * (int)
   * Number of unique brokers to create.  This is a required field.
   * Broker IDs must be numbered from 0 to numBrokers - 1.
   */
  public static Long numBrokers;
  public static int getNumBrokers() {
    Long key = numBrokers;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s = BasePrms.nameForKey(numBrokers) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of unique broker names to create.  Defaults to {@link #numBrokers},
   * which makes every broker name unique.
   */
  public static Long numBrokerNames;
  public static int getNumBrokerNames() {
    Long key = numBrokerNames;
    int numNames = tab().intAt(key, getNumBrokers());
    if (numNames > getNumBrokers()) {
      String s = BasePrms.nameForKey(numBrokerNames) + " cannot be larger than " + BasePrms.nameForKey(numBrokers) + ".";
      throw new HydraConfigException(s);
    }
    return numNames;
  }

  /**
   * (int)
   * Number of tickets to create per broker.  Defaults to 1.
   */
  public static Long numTicketsPerBroker;
  public static int getNumTicketsPerBroker() {
    Long key = numTicketsPerBroker;
    int ntpb = tab().intAt(key, 1);
    if (ntpb < 0) {
      String s = BasePrms.nameForKey(numTicketsPerBroker) + " cannot be less than 0.";
      throw new HydraConfigException(s);
    }
    return ntpb;
  }

  /**
   * (int)
   * Number of unique ticket prices to create.  Defaults to {@link #numBrokers}
   * * {@link #numTicketsPerBroker}.
   */
  public static Long numTicketPrices;
  public static int getNumTicketPrices() {
    Long key = numTicketPrices;
    int ntp = tab().intAt(key, getNumBrokers() * getNumTicketsPerBroker());
    if (ntp < 0 || ntp > (getNumBrokers() * getNumTicketsPerBroker())) {
      String s = BasePrms.nameForKey(numTicketPrices) + " cannot be less than 0 and cannot be greater than " + BasePrms.nameForKey(numBrokers) + " * " + BasePrms.nameForKey(numTicketsPerBroker) + ".";
      throw new HydraConfigException(s);
    }
    return ntp;
  }

  /**
   * (int)
   * Desired size of each query result set.  Defaults to 1.
   */
  public static Long resultSetSize;
  public static int getResultSetSize() {
    Long key = resultSetSize;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(resultSetSize) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Desired size of each query result set.  Defaults to 1.
   */
  public static Long resultSetPercentage;
  public static int getResultSetPercentage() {
    Long key = resultSetPercentage;
    int percentage = tab().intAt(key, 1);
    if (percentage < 0) {
      String s = BasePrms.nameForKey(resultSetPercentage) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return percentage;
  }

  public static Long rangeMinValue;
  public static Long rangeMaxValue;

  static {
    setValues(BrokerPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}
