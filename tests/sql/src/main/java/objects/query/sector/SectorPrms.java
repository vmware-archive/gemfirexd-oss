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
package objects.query.sector;

import hydra.BasePrms;
import hydra.HydraConfigException;
import hydra.HydraVector;
import hydra.RegionPrms;

import java.util.Vector;

import objects.query.QueryPrms;

/**
 * A class used to store keys for test configuration settings.
 */
public class SectorPrms extends BasePrms {

  //numrisk = num positions
  //num positions = num instruments * numPositions per instrument
  //num instruments = num sector * num instruments per sector 

  public static final int POSITION_TYPE = 1;
  public static final int INSTRUMENT_TYPE = 2;
  public static final int SECTOR_TYPE = 3;
  
  public static final int NO_QUERY = -1;

  public static final String RANDOM_POSITION_PK =
      "randomPositionPK";
  public static final String SECTOR_FILTER_IN = "sectorFilterIn";
  public static final String SECTOR_FILTER_IN_AND_MARKET_CAP = "sectorFilterInAndMarketCap";
  public static final String POSITION_PRUNE_BY_INSTRUMENT =
      "positionPruneByInstrument";
  public static final String POSITION_AMOUNT_RANGE_AND =
      "positionAmountRangeAnd";
  public static final String POSITION_AMOUNT_RANGE_OR =
      "positionAmountRangeOr";
  public static final String POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT =
      "positionPruneByInstrumentAndPositionAmount";
  public static final String POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER =
      "positionPruneByInstrumentAndFilterSymbolAndOwner";
  public static final String JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME =
      "joinPruneByPositionAmountAndInstrumentName";
  public static final String JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME =
      "joinPruneByPositionAmountAndInstrumentNameAndSectorName";
  public static final String INSTRUMENT_IN = "instrumentIn";
  public static final String JOIN_INSTRUMENT_AND_POSITION_IN =
    "joinInstrumentAndPositionIn";
  public static final String POSITION_FILTER_IN =
    "positionFilterIn";
  public static final String UPDATE_POSITION_BY_POSITION_ID =
    "updatePositionByPositionId";
  public static final String INSERT_INTO_POSITION = "insertIntoPosition";
  public static final String JOIN_TO_SINGLE_NODE = "joinToSingleNode";
  public static final String JOIN_INSTRUMENT_AND_POSITION_OR =
    "joinInstrumentAndPositionOr";  
  
  public static final int RANDOM_POSITION_PK_QUERY = 0;
  public static final int SECTOR_FILTER_IN_QUERY = 1;
  public static final int SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY = 2;
  public static final int POSITION_PRUNE_BY_INSTRUMENT_QUERY = 3;
  public static final int POSITION_AMOUNT_RANGE_AND_QUERY = 15;
  public static final int POSITION_AMOUNT_RANGE_OR_QUERY = 16;
  public static final int POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY =
      4;
  public static final int POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY =
      5;
  public static final int JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY =
      6;
  public static final int JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY =
      7;
  public static final int INSTRUMENT_IN_QUERY = 8;
  public static final int JOIN_INSTRUMENT_AND_POSITION_IN_QUERY = 9;
  public static final int POSITION_FILTER_IN_QUERY = 10;
  public static final int UPDATE_POSITION_BY_POSITION_ID_QUERY= 11;
  public static final int INSERT_INTO_POSITION_QUERY=12;
  public static final int JOIN_TO_SINGLE_NODE_QUERY = 13;
  public static final int JOIN_INSTRUMENT_AND_POSITION_OR_QUERY = 14;
  //------------------------------------------------------------------
  //Index and Primary Keys
  //------------------------------------------------------------------
  protected static final String PRIMARY_KEY_INDEX_ON_SECTOR_ID =
      "primaryKeyIndexOnSectorId";
  protected static final String UNIQUE_INDEX_ON_SECTOR_ID =
    "uniqueKeyIndexOnSectorId";
  protected static final String PRIMARY_KEY_INDEX_ON_SECTOR_NAME =
    "primaryKeyIndexOnSectorName";
  protected static final String UNIQUE_INDEX_ON_SECTOR_NAME =
    "uniqueKeyIndexOnSectorName";
  protected static final String PRIMARY_KEY_INDEX_ON_MARKET_CAP =
    "primaryKeyIndexOnMarketCap";
  protected static final String UNIQUE_INDEX_ON_MARKET_CAP =
    "uniqueKeyIndexOnMarketCap";
  
  protected static final String PRIMARY_KEY_INDEX_ON_INSTRUMENT_ID =
    "primaryKeyIndexOnInstrumentId";
  protected static final String UNIQUE_INDEX_ON_INSTRUMENT_ID =
    "uniqueKeyIndexOnInstrumentId";
  
  protected static final String PRIMARY_KEY_INDEX_ON_POSITION_ID =
    "primaryKeyIndexOnPositionId";
  protected static final String UNIQUE_INDEX_ON_POSITION_ID =
    "uniqueKeyIndexOnPositionId";
  protected static final String PRIMARY_KEY_INDEX_ON_BOOK_ID =
    "primaryKeyIndexOnBookId";
  protected static final String UNIQUE_INDEX_ON_BOOK_ID =
    "uniqueKeyIndexOnBookId";
  protected static final String PRIMARY_KEY_INDEX_ON_AMOUNT =
    "primaryKeyIndexOnAmount";
  protected static final String UNIQUE_INDEX_ON_AMOUNT =
    "uniqueKeyIndexOnAmount";
  protected static final String PRIMARY_KEY_INDEX_ON_SYNTHETIC =
    "primaryKeyIndexOnSynthetic";
  protected static final String UNIQUE_INDEX_ON_SYNTHETIC =
    "uniqueKeyIndexOnSynthetic";
  protected static final String PRIMARY_KEY_INDEX_ON_OWNER =
    "primaryKeyIndexOnOwner";
  protected static final String UNIQUE_INDEX_ON_OWNER =
    "uniqueKeyIndexOnOwner";
  protected static final String PRIMARY_KEY_INDEX_ON_SYMBOL =
    "primaryKeyIndexOnSymbol";
  protected static final String UNIQUE_INDEX_ON_SYMBOL =
    "uniqueKeyIndexOnSymbol";
  
  protected static final String PRIMARY_KEY_INDEX_ON_RISK_ID =
    "primaryKeyIndexOnRiskId";
  protected static final String UNIQUE_INDEX_ON_RISK_ID =
    "uniqueKeyIndexOnRiskId";
  
  protected static final String INDEX_ON_SECTOR_ID = "indexOnSectorId";
  protected static final String INDEX_ON_INSTRUMENT_ID = "indexOnInstrumentId";
  protected static final String INDEX_ON_POSITION_ID = "indexOnPositionId";
  protected static final String INDEX_ON_POSITION_SYNTHETIC =
      "indexOnPositionSynthetic";
  protected static final String INDEX_ON_POSITION_INSTRUMENT =
      "indexOnPositionInstrument";
  protected static final String INDEX_ON_POSITION_SYMBOL =
      "indexOnPositionSymbol";
  protected static final String INDEX_ON_POSITION_OWNER =
      "indexOnPositionOwner";
  protected static final String INDEX_ON_POSITION_AMOUNT =
      "indexOnPositionAmount";
  protected static final String INDEX_ON_SECTOR_NAME = "indexOnSectorName";
  protected static final String INDEX_ON_SECTOR_MARKET_CAP =
      "indexOnSectorMarketCap";

  protected static final int PRIMARY_KEY_INDEX_ON_SECTOR_ID_QUERY = 0;
  protected static final int UNIQUE_INDEX_ON_SECTOR_ID_QUERY = 28;
  protected static final int PRIMARY_KEY_INDEX_ON_SECTOR_NAME_QUERY = 29;
  protected static final int UNIQUE_INDEX_ON_SECTOR_NAME_QUERY = 1;
  protected static final int PRIMARY_KEY_INDEX_ON_MARKET_CAP_QUERY = 30;
  protected static final int UNIQUE_INDEX_ON_MARKET_CAP_QUERY = 31;
  protected static final int PRIMARY_KEY_INDEX_ON_INSTRUMENT_ID_QUERY = 2;
  protected static final int UNIQUE_INDEX_ON_INSTRUMENT_ID_QUERY = 27;
  protected static final int PRIMARY_KEY_INDEX_ON_POSITION_ID_QUERY = 3;
  protected static final int UNIQUE_INDEX_ON_POSITION_ID_QUERY = 4;
  
  protected static final int PRIMARY_KEY_INDEX_ON_BOOK_ID_QUERY = 5;
  protected static final int UNIQUE_INDEX_ON_BOOK_ID_QUERY = 6;
  protected static final int PRIMARY_KEY_INDEX_ON_SYNTHETIC_QUERY = 7;
  protected static final int UNIQUE_INDEX_ON_SYNTHETIC_QUERY = 8;
  protected static final int PRIMARY_KEY_INDEX_ON_SYMBOL_QUERY = 9;
  protected static final int UNIQUE_INDEX_ON_SYMBOL_QUERY = 10;
  protected static final int PRIMARY_KEY_INDEX_ON_AMOUNT_QUERY = 11;
  protected static final int UNIQUE_INDEX_ON_AMOUNT_QUERY = 12;
  protected static final int PRIMARY_KEY_INDEX_ON_OWNER_QUERY = 13;
  protected static final int UNIQUE_INDEX_ON_OWNER_QUERY = 14;
  protected static final int PRIMARY_KEY_INDEX_ON_RISK_ID_QUERY = 15;
  protected static final int UNIQUE_INDEX_ON_RISK_ID_QUERY = 16;
  
  protected static final int INDEX_ON_SECTOR_ID_QUERY = 17;
  protected static final int INDEX_ON_INSTRUMENT_ID_QUERY = 18;
  protected static final int INDEX_ON_POSITION_ID_QUERY = 19;
  protected static final int INDEX_ON_POSITION_SYNTHETIC_QUERY = 20;
  protected static final int INDEX_ON_POSITION_INSTRUMENT_QUERY = 21;
  protected static final int INDEX_ON_POSITION_SYMBOL_QUERY = 22;
  protected static final int INDEX_ON_POSITION_OWNER_QUERY = 23;
  protected static final int INDEX_ON_POSITION_AMOUNT_QUERY = 24;
  protected static final int INDEX_ON_SECTOR_NAME_QUERY = 25;
  protected static final int INDEX_ON_SECTOR_MARKET_CAP_QUERY = 26;

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
    if (val.equals(PRIMARY_KEY_INDEX_ON_SECTOR_ID)) {
      return PRIMARY_KEY_INDEX_ON_SECTOR_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_SECTOR_ID)) {
      return UNIQUE_INDEX_ON_SECTOR_ID_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_SECTOR_NAME)) {
      return PRIMARY_KEY_INDEX_ON_SECTOR_NAME_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_SECTOR_NAME)) {
      return UNIQUE_INDEX_ON_SECTOR_NAME_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_MARKET_CAP)) {
      return PRIMARY_KEY_INDEX_ON_MARKET_CAP_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_SECTOR_NAME)) {
      return UNIQUE_INDEX_ON_SECTOR_NAME_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_INSTRUMENT_ID)) {
      return PRIMARY_KEY_INDEX_ON_INSTRUMENT_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_INSTRUMENT_ID)) {
      return UNIQUE_INDEX_ON_INSTRUMENT_ID_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_POSITION_ID)) {
      return PRIMARY_KEY_INDEX_ON_POSITION_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_POSITION_ID)) {
      return UNIQUE_INDEX_ON_POSITION_ID_QUERY;
    }
    
    else if (val.equals(PRIMARY_KEY_INDEX_ON_OWNER)) {
      return PRIMARY_KEY_INDEX_ON_OWNER_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_OWNER)) {
      return UNIQUE_INDEX_ON_OWNER_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_SYNTHETIC)) {
      return PRIMARY_KEY_INDEX_ON_SYNTHETIC_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_SYNTHETIC)) {
      return UNIQUE_INDEX_ON_SYNTHETIC_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_AMOUNT)) {
      return PRIMARY_KEY_INDEX_ON_AMOUNT_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_AMOUNT)) {
      return UNIQUE_INDEX_ON_AMOUNT_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_SYMBOL)) {
      return PRIMARY_KEY_INDEX_ON_SYMBOL_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_SYMBOL)) {
      return UNIQUE_INDEX_ON_SYMBOL_QUERY;
    }
    else if (val.equals(PRIMARY_KEY_INDEX_ON_BOOK_ID)) {
      return PRIMARY_KEY_INDEX_ON_BOOK_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_BOOK_ID)) {
      return UNIQUE_INDEX_ON_BOOK_ID_QUERY;
    }
    
    else if (val.equals(PRIMARY_KEY_INDEX_ON_RISK_ID)) {
      return PRIMARY_KEY_INDEX_ON_RISK_ID_QUERY;
    }
    else if (val.equals(UNIQUE_INDEX_ON_RISK_ID)) {
      return UNIQUE_INDEX_ON_RISK_ID_QUERY;
    }
    
    else if (val.equals(INDEX_ON_SECTOR_ID)) {
      return INDEX_ON_SECTOR_ID_QUERY;
    }
    else if (val.equals(INDEX_ON_INSTRUMENT_ID)) {
      return INDEX_ON_INSTRUMENT_ID_QUERY;
    }
    else if (val.equals(INDEX_ON_POSITION_ID)) {
      return INDEX_ON_POSITION_ID_QUERY;
    }
    else if (val.equals(INDEX_ON_POSITION_SYNTHETIC)) {
      return INDEX_ON_POSITION_SYNTHETIC_QUERY;
    }
    else if (val.equals(INDEX_ON_POSITION_INSTRUMENT)) {
      return INDEX_ON_POSITION_INSTRUMENT_QUERY;
    }
    else if (val.equals(INDEX_ON_POSITION_SYMBOL)) {
      return INDEX_ON_POSITION_SYMBOL_QUERY;
    }
    else if (val.equals(INDEX_ON_POSITION_OWNER)) {
      return INDEX_ON_POSITION_OWNER_QUERY;
    }
    else if (val.equals(INDEX_ON_POSITION_AMOUNT)) {
      return INDEX_ON_POSITION_AMOUNT_QUERY;
    }
    else if (val.equals(INDEX_ON_SECTOR_NAME)) {
      return INDEX_ON_SECTOR_NAME_QUERY;
    }
    else if (val.equals(INDEX_ON_SECTOR_MARKET_CAP)) {
      return INDEX_ON_SECTOR_MARKET_CAP_QUERY;
    }
    else if (val.equals(NONE)) {
      return NO_QUERY;
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
    switch (api) {
      case QueryPrms.GFXD:
        return getSQLQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(RANDOM_POSITION_PK)) {
      return RANDOM_POSITION_PK_QUERY;
    }
    else if (val.equalsIgnoreCase(SECTOR_FILTER_IN)) {
      return SECTOR_FILTER_IN_QUERY;
    }
    else if (val.equalsIgnoreCase(SECTOR_FILTER_IN_AND_MARKET_CAP)) {
      return SECTOR_FILTER_IN_AND_MARKET_CAP_QUERY;
    }
    else if (val.equalsIgnoreCase(POSITION_PRUNE_BY_INSTRUMENT)) {
      return POSITION_PRUNE_BY_INSTRUMENT_QUERY;
    }
    else if (val.equalsIgnoreCase(POSITION_AMOUNT_RANGE_AND)) {
      return POSITION_AMOUNT_RANGE_AND_QUERY;
    }
    else if (val.equalsIgnoreCase(POSITION_AMOUNT_RANGE_OR)) {
      return POSITION_AMOUNT_RANGE_OR_QUERY;
    }
    else if (val.equalsIgnoreCase(POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT)) {
      return POSITION_PRUNE_BY_INSTRUMENT_AND_POSITION_AMOUNT_QUERY;
    }
    else if (val
        .equalsIgnoreCase(POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER)) {
      return POSITION_PRUNE_BY_INSTRUMENT_AND_FILTER_SYMBOL_AND_OWNER_QUERY;
    }
    else if (val.equalsIgnoreCase(JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME)) {
      return JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_QUERY;
    }
    else if (val
        .equalsIgnoreCase(JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME)) {
      return JOIN_PRUNE_BY_POSITION_AMOUNT_AND_INSTRUMENT_NAME_AND_SECTOR_NAME_QUERY;
    }
    else if (val.equalsIgnoreCase(INSTRUMENT_IN)) {
      return INSTRUMENT_IN_QUERY;
    }
    else if (val.equalsIgnoreCase(JOIN_INSTRUMENT_AND_POSITION_IN)) {
      return JOIN_INSTRUMENT_AND_POSITION_IN_QUERY;
    }   
    else if (val.equalsIgnoreCase(POSITION_FILTER_IN)) {
      return POSITION_FILTER_IN_QUERY;
    }
    else if (val.equalsIgnoreCase(UPDATE_POSITION_BY_POSITION_ID)) {
      return UPDATE_POSITION_BY_POSITION_ID_QUERY;
    }
    else if (val.equalsIgnoreCase(INSERT_INTO_POSITION)) {
      return INSERT_INTO_POSITION_QUERY;
    }
    else if (val.equalsIgnoreCase(JOIN_TO_SINGLE_NODE)) {
      return JOIN_TO_SINGLE_NODE_QUERY;
    }
    else if (val.equalsIgnoreCase(JOIN_INSTRUMENT_AND_POSITION_OR)) {
      return JOIN_INSTRUMENT_AND_POSITION_OR_QUERY;
    }
    else {
      String s =
          "Unsupported value for " + QueryPrms.getAPIString(QueryPrms.GFXD)
              + " query " + BasePrms.nameForKey(key) + ": " + val;
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
    switch (api) {
      case QueryPrms.GFXD:
        return getSQLUpdateQueryType(key, val);
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  private static int getSQLUpdateQueryType(Long key, String val) {
    if (val.equalsIgnoreCase(UPDATE_POSITION_BY_POSITION_ID)) {
      return UPDATE_POSITION_BY_POSITION_ID_QUERY;
    }
    else {
      String s =
          "Unsupported value for " + QueryPrms.getAPIString(QueryPrms.GFXD)
              + " query " + BasePrms.nameForKey(key) + ": " + val;
      throw new HydraConfigException(s);
    }
  }
  
  /**
   * (String(s))
   * Type of query.
   */
  public static Long deleteQueryType;

  public static int getDeleteQueryType(int api) {
    Long key = deleteQueryType;
    String val = tasktab().stringAt(key, tab().stringAt(key));
    switch (api) {
      default:
        String s = "Unsupported API: " + QueryPrms.getAPIString(api);
        throw new HydraConfigException(s);
    }
  }

  /**
   * (String(s))
   * Fields to return in result set.  Defaults to "*".
   */
  public static Long sectorFields;

  public static String getSectorFields() {
    Long key = sectorFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return Sector.commaSeparatedStringFor(val);
  }
  
  public static Vector getSectorFieldsAsVector() {
    Long key = sectorFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return val;
  }

  /**
   * (String(s))
   * Fields to return in result set.  Defaults to "*".
   */
  public static Long positionFields;

  public static String getPositionFields() {
    Long key = positionFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return Position.commaSeparatedStringFor(val);
  }

  public static Vector getPositionFieldsAsVector() {
    Long key = positionFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return val;
  }

  /**
   * (String(s))
   * Fields to return in result set.  Defaults to "*".
   */
  public static Long instrumentFields;

  public static String getInstrumentFields() {
    Long key = instrumentFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return Instrument.commaSeparatedStringFor(val);
  }

  public static Vector getInstrumentFieldsAsVector() {
    Long key = instrumentFields;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector("*")));
    return val;
  }

  /**
   * (int)
   * Number of unique sectors to create.  This is a required field.
   * Sector IDs must be numbered from 0 to numSectors - 1.
   */
  public static Long numSectors;

  public static int getNumSectors() {
    Long key = numSectors;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s = BasePrms.nameForKey(numSectors) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of instruments per sector to create.  This is a required field.
   */
  public static Long numInstrumentsPerSector;

  public static int getNumInstrumentsPerSector() {
    Long key = numInstrumentsPerSector;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s =
          BasePrms.nameForKey(numInstrumentsPerSector) + " must be positive: "
              + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of positions per instrument to create.  This is a required field.
   */
  public static Long numPositionsPerInstrument;

  public static int getNumPositionsPerInstrument() {
    Long key = numPositionsPerInstrument;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s =
          BasePrms.nameForKey(numPositionsPerInstrument)
              + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * calculates the number of total instruments based on (@link SectorPrms.numInstrumentsPerSector}
   * and {@link SectorPrms.numSectors}
   * @return
   */
  public static int getNumInstruments() {
    return SectorPrms.getNumSectors() * SectorPrms.getNumInstrumentsPerSector();
  }

  /**
   * calculates the number of total positions based on the configuration provided by {@link SectorPrms}
   */
  public static int getNumPositions() {
    return getNumInstruments() * SectorPrms.getNumPositionsPerInstrument();
  }

  /**
   * The total number of book values to create
   */
  public static Long numBookValues;

  public static int getNumBookValues() {
    Long key = numBookValues;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s = BasePrms.nameForKey(numBookValues) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of unique marketCap values to create.  This is a required field.
   */
  public static Long numMarketCapValues;

  public static int getNumMarketCapValues() {
    Long key = numMarketCapValues;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s =
          BasePrms.nameForKey(numMarketCapValues) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * The number of distinct risk values to use.  
   */
  public static Long numRiskValues;

  public static int getNumRiskValues() {
    Long key = numRiskValues;
    int val = tab().intAt(key);
    if (val <= 0) {
      String s =
          BasePrms.nameForKey(numRiskValues) + " must be positive: " + val;
      throw new HydraConfigException(s);
    }
    return val;
  }

  /**
   * (int)
   * Number of symbol values to use.  Defaults to 1.
   */
  public static Long numSymbolValues;

  public static int getNumSymbolValues() {
    Long key = numSymbolValues;
    int val = tab().intAt(key, 1);
    if (val < 0) {
      String s = BasePrms.nameForKey(numSymbolValues) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return val;
  }

  //----
  //Query Sizing Params
  //----

  /**
   * (int)
   * Number of amounts to put in the "range" clause for a query.  Defaults to 1.
   */
  public static Long numAmountsPerRangeClause;

  public static int getNumAmountsPerRangeClause() {
    Long key = numAmountsPerRangeClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numAmountsPerRangeClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Number of instruments to put in the "in" clause for a query.  Defaults to 1.
   */
  public static Long numInstrumentsPerInClause;

  public static int getNumInstrumentsPerInClause() {
    Long key = numInstrumentsPerInClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numInstrumentsPerInClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Number of market cap values to put in the "or" clause for a query.  Defaults to 1.
   */
  public static Long numMarketCapValuesPerOrClause;

  public static int getNumMarketCapValuesPerOrClause() {
    Long key = numMarketCapValuesPerOrClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numMarketCapValuesPerOrClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Number of owners to put in the "or" clause for a query.  Defaults to 1.
   */
  public static Long numOwnersPerOrClause;

  public static int getNumOwnersPerOrClause() {
    Long key = numOwnersPerOrClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numOwnersPerOrClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Number of positions to put in the "in" clause for a query.  Defaults to 1.
   */
  public static Long numPositionsPerInClause;

  public static int getNumPositionsPerInClause() {
    Long key = numPositionsPerInClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numPositionsPerInClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Number of sectors to put in the "in" clause for a query.  Defaults to 1.
   */
  public static Long numSectorsPerInClause;

  public static int getNumSectorsPerInClause() {
    Long key = numSectorsPerInClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numSectorsPerInClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

  /**
   * (int)
   * Number of symbols to put in the "in" clause for a query.  Defaults to 1.
   */
  public static Long numSymbolsPerInClause;

  public static int getNumSymbolsPerInClause() {
    Long key = numSymbolsPerInClause;
    int size = tab().intAt(key, 1);
    if (size < 0) {
      String s = BasePrms.nameForKey(numSymbolsPerInClause) + " cannot be negative.";
      throw new HydraConfigException(s);
    }
    return size;
  }

//------------------------------------------------------------------------------

  /**
   * Data Policy configuration from {@link SectorPrms#sectorDataPolicy}
   * For SQL Sector Table;
   */
  public static Long sectorDataPolicy;

  public static int getSectorDataPolicy() {
    Long key = sectorDataPolicy;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }

  /**
   * Data Policy configuration from {@link SectorPrms#instrumentDataPolicy}
   * For SQL Instrument Table;
   */
  public static Long instrumentDataPolicy;

  public static int getInstrumentDataPolicy() {
    Long key = instrumentDataPolicy;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }

  /**
   * (String(s))
   * Data Policy configuration from {@link SectorPrms#positionDataPolicy}
   * For SQL Position Table;
   */
  public static Long positionDataPolicy;

  public static int getPositionDataPolicy() {
    Long key = positionDataPolicy;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }

  /**
   * (String(s))
   * Data Policy configuration from {@link SectorPrms#riskDataPolicy}
   * For SQL Risk Table
   */
  public static Long riskDataPolicy;
  public static int getRiskDataPolicy() {
    Long key = riskDataPolicy;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.NO_DATA_POLICY));
    return QueryPrms.getDataPolicy(key, val);
  }

  /**
   * Partition redundancy for the SQL Sector table.
   */
  public static Long sectorPartitionRedundancy;

  public static int getSectorPartitionRedundancy() {
    Long key = sectorPartitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key));
  }
  /**
   * Partition redundancy for the SQL Instrument table.
   */
  public static Long instrumentPartitionRedundancy;

  public static int getInstrumentPartitionRedundancy() {
    Long key = instrumentPartitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key));
  }
  /**
   * Partition redundancy for the SQL Position table.
   */
  public static Long positionPartitionRedundancy;

  public static int getPositionPartitionRedundancy() {
    Long key = positionPartitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key));
  }
  /**
   * Partition redundancy for the SQL Risk table.
   */
  public static Long riskPartitionRedundancy;

  public static int getRiskPartitionRedundancy() {
    Long key = riskPartitionRedundancy;
    return tasktab().intAt(key, tab().intAt(key));
  }

  /**
   * Whether to use offheap for the SQL Sector table. Defaults to false.
   */
  public static Long sectorOffHeap;

  public static boolean getSectorOffHeap() {
    Long key = sectorOffHeap;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Whether to use offheap for the SQL Instrument table. Defaults to false.
   */
  public static Long instrumentOffHeap;

  public static boolean getInstrumentOffHeap() {
    Long key = instrumentOffHeap;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Whether to use offheap for the SQL Position table. Defaults to false.
   */
  public static Long positionOffHeap;

  public static boolean getPositionOffHeap() {
    Long key = positionOffHeap;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Whether to use offheap for the SQL Risk table. Defaults to false.
   */
  public static Long riskOffHeap;

  public static boolean getRiskOffHeap() {
    Long key = riskOffHeap;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
 
  /**
   * Total number of buckets in the SQL Sector table.  Defaults to 0, which
   * uses the product default.
   */
  public static Long sectorPartitionTotalNumBuckets;

  public static int getSectorPartitionTotalNumBuckets() {
    Long key = sectorPartitionTotalNumBuckets;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
  /**
   * Total number of buckets in the SQL Instrument table.  Defaults to 0, which
   * uses the product default.
   */
  public static Long instrumentPartitionTotalNumBuckets;

  public static int getInstrumentPartitionTotalNumBuckets() {
    Long key = instrumentPartitionTotalNumBuckets;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
  /**
   * Total number of buckets in the SQL Position table.  Defaults to 0, which
   * uses the product default.
   */
  public static Long positionPartitionTotalNumBuckets;

  public static int getPositionPartitionTotalNumBuckets() {
    Long key = positionPartitionTotalNumBuckets;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }
  /**
   * Total number of buckets in the SQL Risk table.  Defaults to 0, which
   * uses the product default.
   */
  public static Long riskPartitionTotalNumBuckets;

  public static int getRiskPartitionTotalNumBuckets() {
    Long key = riskPartitionTotalNumBuckets;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * Data Policy configuration from {@link SectorPrms#sectorDataPolicy}
   * For SQL Sector Table;
   */
  public static Long sectorPartitionType;

  public static int getSectorPartitionType() {
    Long key = sectorPartitionType;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, QueryPrms.PARTITION_BY_PK_TYPE ));
    return QueryPrms.getPartitionType(key, val);
  }
  
  /**
   * Data Policy configuration from {@link SectorPrms#sectorDataPolicy}
   * For SQL Instrument Table;
   */
  public static Long instrumentPartitionType;

  public static int getInstrumentPartitionType() {
    Long key = instrumentPartitionType;
    String val =
        tasktab().stringAt(key, tab().stringAt(key,  QueryPrms.PARTITION_BY_PK_TYPE ));
    return QueryPrms.getPartitionType(key, val);
  }
  
  /**
   * Data Policy configuration from {@link SectorPrms#sectorDataPolicy}
   * For SQL Position Table;
   */
  public static Long positionPartitionType;

  public static int getPositionPartitionType() {
    Long key = positionPartitionType;
    String val =
        tasktab().stringAt(key, tab().stringAt(key,  QueryPrms.PARTITION_BY_PK_TYPE ));
    return QueryPrms.getPartitionType(key, val);
  }
  
  /**
   * Partition type from configuration from {@link SectorPrms#sectorDataPolicy} when set to {@link QueryPrms#PARTITION}
   * For SQL Risk Table;
   */
  public static Long riskPartitionType;

  public static int getRiskPartitionType() {
    Long key = riskPartitionType;
    String val =
        tasktab().stringAt(key, tab().stringAt(key,  QueryPrms.PARTITION_BY_PK_TYPE ));
    return QueryPrms.getPartitionType(key, val);
  }
  
  /**
   * Column to partition on.
   * For SQL Sector Table;
   */
  public static Long sectorPartitionColumn;

  public static String getSectorPartitionColumn() {
    Long key = sectorPartitionColumn;
    String val =
        tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }
  
  /**
   * Column to partition on.
   * For SQL Instrument Table;
   */
  public static Long instrumentPartitionColumn;

  public static String getInstrumentPartitionColumn() {
    Long key = instrumentPartitionColumn;
    String val =
        tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }
  
  /**
   * Data Policy configuration from {@link SectorPrms#sectorDataPolicy}
   * For SQL Position Table;
   */
  public static Long positionPartitionColumn;

  public static String getPositionPartitionColumn() {
    Long key = positionPartitionColumn;
    String val =
        tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }
  
  /**
   * Column to partition on.
   * For SQL Risk Table;
   */
  public static Long riskPartitionColumn;

  public static String getRiskPartitionColumn() {
    Long key = riskPartitionColumn;
    String val =
        tasktab().stringAt(key, tab().stringAt(key));
    return val;
  }
  
  /**
   * Column to partition on.
   * For SQL Sector Table;
   */
  public static Long sectorCreateTableIndexes;

  public static Vector getSectorCreateTableIndexes() {
    Long key = sectorCreateTableIndexes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }
  
  /**
   * Column to partition on.
   * For SQL Instrument Table;
   */
  public static Long instrumentCreateTableIndexes;

  public static Vector getInstrumentCreateTableIndexes() {
    Long key = instrumentCreateTableIndexes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }
  
  /**
   * Data Policy configuration from {@link SectorPrms#sectorDataPolicy}
   * For SQL Position Table;
   */
  public static Long positionCreateTableIndexes;

  public static Vector getPositionCreateTableIndexes() {
    Long key = positionCreateTableIndexes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }
  
  /**
   * Column to partition on.
   * For SQL Risk Table;
   */
  public static Long riskCreateTableIndexes;

  public static Vector getRiskCreateTableIndexes() {
    Long key = riskCreateTableIndexes;
    Vector val = tasktab().vecAt(key, tab().vecAt(key, new HydraVector()));
    return val;
  }
  
  public static Long sectorColocationStatement;
  
  public static String getSectorColocationStatement() {
    Long key = sectorColocationStatement;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, ""));
    if (val == NONE) {
      val = "";
    }
    return val;
  }

  public static Long instrumentColocationStatement;
  
  public static String getInstrumentColocationStatement() {
    Long key = instrumentColocationStatement;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, ""));
    if (val.equals(NONE)) {
      val = "";
    }
    return val;
  }
  
  public static Long positionColocationStatement;
  
  public static String getPositionColocationStatement() {
    Long key = positionColocationStatement;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, ""));
    if (val.equals(NONE)) {
      val = "";
    }
    return val;
  }
  
  public static Long riskColocationStatement;
  
  public static String getRiskColocationStatement() {
    Long key = riskColocationStatement;
    String val =
        tasktab().stringAt(key, tab().stringAt(key, ""));
    if (val.equals(NONE)) {
      val = "";
    }
    return val;
  }
  
  /**
   * (boolean)
   * Whether to commit batches during data creation.  Defaults to true.
   * Note that no commits are done if the transaction isolation level is NONE.
   */
  public static Long commitBatches;
  public static boolean commitBatches() {
    Long key = commitBatches;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * (int) the number of client vms that the test will use
   */
  public static Long numClients;
  public static int getNumClients() {
    Long key = numClients;
    return tasktab().intAt(key, tab().intAt(key));
  }
  
  /**
   * (int) the number of server vms that the test will use
   */
  public static Long numServers;
  public static int getNumServers() {
    Long key = numServers;
    return tasktab().intAt(key, tab().intAt(key));
  }
  
  static {
    setValues(SectorPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
}
