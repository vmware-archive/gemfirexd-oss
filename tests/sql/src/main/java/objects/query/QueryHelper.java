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

package objects.query;

import hydra.HydraConfigException;

/**
 * Creates instances of objects that implement {@link QueryFactory}.
 *  <p>
 *  Usage example:
 *  <blockquote>
 *  <pre>
 *     // In a hydra configuration file...
 *     objects.query.QueryPrms-api = sql;
 *     objects.query.QueryPrms-objectType = objects.query.broker.Broker;
 *     objects.query.broker.BrokerPrms-queryType =
 *               randomEqualityQueryOnBrokerId;
 *
 *     // In a hydra client vm...
 *     String api = QueryPrms.getAPI();
 *     String objectType = QueryPrms.getObjectType();
 *     QueryFactory queryFactory = QueryHelper.getQueryFactory(api, objectType);
 *     for (int i = 0; i < 500; i++) {
 *       if (queryFactory instanceof OQLQueryFactory) {
 *         OQLQueryFactory oqlQueryFactory = (OQLQueryFactory)queryFactory;
 *         oqlQueryFactory.createRegions();
 *         ....
 *       } else if (queryFactory instanceof SQLQueryFactory) {
 *         SQLQueryFactory sqlQueryFactory = (SQLQueryFactory)queryFactory;
 *         sqlQueryFactory.getTableStatements();
 *         ....
 *       }
 *       // Uses factory-specific parameter class
 *       String queryType = queryFactory.getQueryType();
 *       String query = queryFactory.getQuery(queryType, i);
 *       ...
 *     }
 *  </pre>
 *  </blockquote>
 */

public class QueryHelper {

  /**
   * Returns a factory for the specified API and object type.
   *
   * @throws HydraConfigException
   *         The factory for the API and objectType cannot be found.
   */
  public static QueryFactory getQueryFactory(int api, String objectType) {
    QueryFactory factory = null;
    if (objectType.equals(QueryPrms.BROKER_OBJECT_TYPE)) {
      factory = objects.query.broker.Broker.getQueryFactory(api);
      if (factory == null) {
        String s = api + " factory not found for " + objectType;
        throw new HydraConfigException(s);
      }
    }
    else if (objectType.equals(QueryPrms.SECTOR_OBJECT_TYPE)) {
      factory = objects.query.sector.Sector.getQueryFactory(api);
      if (factory == null) {
        String s = api + " factory not found for " + objectType;
        throw new HydraConfigException(s);
      }
    }
    else if (objectType.equals(QueryPrms.LARGE_OBJECT_OBJECT_TYPE)) {
      factory = objects.query.largeobject.LargeObject.getQueryFactory(api);
      if (factory == null) {
        String s = api + " factory not found for " + objectType;
        throw new HydraConfigException(s);
      }
    }
    else if (objectType.equals(QueryPrms.TINY_OBJECT_OBJECT_TYPE)) {
      factory = objects.query.tinyobject.TinyObject.getQueryFactory(api);
      if (factory == null) {
        String s = api + " factory not found for " + objectType;
        throw new HydraConfigException(s);
      }
    }
    else {
      String s = "Factory not found: " + objectType;
      s += " check QueryHelper to make sure object type has been added.";
      throw new HydraConfigException(s);
    }

    factory.init();

    return factory;
  }
}
