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
package com.gemstone.gemfire.management.internal.cli.functions;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexInvalidException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.management.internal.cli.domain.IndexInfo;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.configuration.domain.XmlEntity;

/***
 * Function to create index in a member, based on different arguments passed to it
 * @author bansods
 *
 */
public class CreateIndexFunction extends FunctionAdapter implements
InternalEntity {


  private static final long serialVersionUID = 1L;

  @Override
  public void execute(FunctionContext context) {
    final IndexInfo indexInfo = (IndexInfo)context.getArguments();
    String memberId = null;
    try {
      Cache cache = CacheFactory.getAnyInstance();
      memberId = cache.getDistributedSystem().getDistributedMember().getId();
      QueryService queryService = cache.getQueryService();
      String indexName = indexInfo.getIndexName();
      String indexedExpression = indexInfo.getIndexedExpression();
      String regionPath = indexInfo.getRegionPath();

      switch (indexInfo.getIndexType()) {
        case IndexInfo.RANGE_INDEX:
          queryService.createIndex(indexName, indexedExpression, regionPath);
          break;
        case IndexInfo.KEY_INDEX:
          queryService.createKeyIndex(indexName, indexedExpression, regionPath);
          break;
        case IndexInfo.HASH_INDEX:
          queryService.createHashIndex(indexName, indexedExpression, regionPath);
          break;
        default :
          queryService.createIndex(indexName, indexedExpression, regionPath);
      }
      
      XmlEntity xmlEntity = new XmlEntity(CacheXml.REGION, "name", cache.getRegion(regionPath).getName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, xmlEntity));
    } catch (IndexExistsException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__INDEX__EXISTS, indexInfo.getIndexName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (IndexNameConflictException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__NAME__CONFLICT, indexInfo.getIndexName());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (RegionNotFoundException e) {
      String message = CliStrings.format(CliStrings.CREATE_INDEX__INVALID__REGIONPATH, indexInfo.getRegionPath());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, false, message));
    } catch (IndexInvalidException e) {
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, e.getMessage()));
    }
    catch (Exception e) {
      String exceptionMessage = CliStrings.format(CliStrings.EXCEPTION_CLASS_AND_MESSAGE, e.getClass().getName(), e.getMessage());
      context.getResultSender().lastResult(new CliFunctionResult(memberId, e, exceptionMessage));
    }
  }

  @Override
  public String getId() {
    return CreateIndexFunction.class.getName();
  }
}
