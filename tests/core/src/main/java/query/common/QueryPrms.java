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
package query.common;

import query.common.QueryPrms;
import hydra.BasePrms;

public class QueryPrms extends BasePrms {

/*
 * various operations that will be done. 
 * 1) all Entry operations
 * 2) Querying
 * 3) Index Creation and removal
 */
public static Long entryAndQueryOperations;  

/*
 * Whether to use the random values or not
 * If false then conf file should specify the object type to use
 */
public static Long useRandomValues;

/*
 * Type of the object to be created. Genreally it will objects.portfolio
 */
public static Long objectType;

/*
 * Number of regions to create.
 */
public static Long numOfRegions;

/**
 * XmlFileName (if cache is to be created from an existing cache.xml file).
 */
public static Long xmlFilename;


/**
 * The query strings which can be specified via conf files
 */
public static Long queryStrings;

/**
 * The expected QueryResult sizes
 */
public static Long expectedQueryResultSizes;

/**
 * The query execution parameters which can be specified via conf files
 */
public static Long queryParameters;

/**
 * This is to indicate the number of parameters to be set with each query.
 * e.g. if there are 2 queries in conf:
 * query.QueryPrms-queryStrings =
 *  "select * from /Region r WHERE r.status= $1 AND r.qty > $2"
 *  "select * from /Region r WHERE r.status = $1" ;
 * The number of parameters that needs to be passed to query1 is 2
 * and for query2 is 1. This info is passed as:
 * query.QueryPrms-queryParametersLength = "2" "1";
 *
 */
public static Long queryParametersLength;

/**
 * The validator class specified, if any
 */
public static Long resultsValidator;

/**
 * The expected size of the Results corresponding to the query
 */
public static Long expectedResultsSize;

/**
 * To signify whether the region is used in Remote Querying 
 */
public static Long regionForRemoteOQL;

/**
 * Expected Results type for the query ( ResultsSet or StructSet at this point ).
 * Needs to be enhanced further to be able to give StructType etc
 */
public static Long expectedResultsType;

/**
 * Use this limit clause in the query operations.
 * The result size is also validated to be <= limit.
 * -1 is the default,implying no limit is to be used.
 * Any positive real number is valid. 
 */
public static Long queryLimit;

/** (boolean) Whether or not to allow the QueryInvocationTargetException (this
 *  should only be allowed in PR Query w/HA tests).
 */
public static Long allowQueryInvocationTargetException;
public static boolean allowQueryInvocationTargetException() {
  Long key = allowQueryInvocationTargetException;
  boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
  return val;
}

/*
 * Number of vms to stopStart.
 */
public static Long totalNumOfVmsToStop;

/*
 * Size of byte[] included in each entry.
 */
public static Long payloadSize;

/*
 * Initial size of the region.
 */
public static Long numEntries;

/*
 * This parameter is specific to serialQueryIndexValidationEntry.conf
 * It tells the test that index usage Validation is to be performed
 * in serial environment.
 */
public static Long isIndexUsageValidation;

/*
 * Some tests involving query monitoring will need that we ignore 
 * queryExecutionTimedOut exception
 */
public static Long ignoreTimeOutException;

// ================================================================================
static {
   BasePrms.setValues(QueryPrms.class);
}

}
