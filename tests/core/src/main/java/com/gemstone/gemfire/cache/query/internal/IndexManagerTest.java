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
 * @author Asif
 */
package com.gemstone.gemfire.cache.query.internal;

import java.util.Iterator;
import java.util.List;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.index.IndexData;
import com.gemstone.gemfire.cache.query.internal.index.IndexUtils;

public class IndexManagerTest extends TestCase
{

  public IndexManagerTest(String testName) {
    super(testName);
  }

  protected void setUp() throws java.lang.Exception
  {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
    for (int i = 0; i < 4; i++) {
      region.put("" + i, new Portfolio(i));
      // System.out.println(new Portfolio(i));
    }

  }

  protected void tearDown() throws java.lang.Exception
  {
    CacheUtils.closeCache();
  }

  public void testBestIndexPick() throws Exception
  {
    QueryService qs;

    qs = CacheUtils.getQueryService();
    qs.createIndex("statusIndex", IndexType.FUNCTIONAL, "status",
        "/portfolios, positions");
    QCompiler compiler = new QCompiler(CacheUtils.getLogger()
        .convertToLogWriterI18n());
    List list = compiler.compileFromClause("/portfolios pf");
    ExecutionContext context = new QueryExecutionContext(null, CacheUtils.getCache());
    context.newScope(context.assosciateScopeID());

    Iterator iter = list.iterator();
    while (iter.hasNext()) {
      CompiledIteratorDef iterDef = (CompiledIteratorDef)iter.next();
      context.addDependencies(new CompiledID("dummy"), iterDef.computeDependencies(context));
      RuntimeIterator rIter = iterDef.getRuntimeIterator(context);
      context.bindIterator(rIter);
      context.addToIndependentRuntimeItrMap(iterDef);
    }
    CompiledPath cp = new CompiledPath(new CompiledID("pf"), "status");

    // TASK ICM1
    String[] defintions = { "/portfolios", "index_iter1.positions" };
    IndexData id = IndexUtils.findIndex("/portfolios", defintions, cp, "*",
        CacheUtils.getCache(), true, context);
    Assert.assertEquals(id.getMatchLevel(), 0);
    Assert.assertEquals(id.getMapping()[0], 1);
    Assert.assertEquals(id.getMapping()[1], 2);
    String[] defintions1 = { "/portfolios" };
    IndexData id1 = IndexUtils.findIndex("/portfolios", defintions1, cp, "*",
        CacheUtils.getCache(), true, context);
    Assert.assertEquals(id1.getMatchLevel(), -1);
    Assert.assertEquals(id1.getMapping()[0], 1);
    String[] defintions2 = { "/portfolios", "index_iter1.positions",
        "index_iter1.coll1" };
    IndexData id2 = IndexUtils.findIndex("/portfolios", defintions2, cp, "*",
        CacheUtils.getCache(), true, context);
    Assert.assertEquals(id2.getMatchLevel(), 1);
    Assert.assertEquals(id2.getMapping()[0], 1);
    Assert.assertEquals(id2.getMapping()[1], 2);
    Assert.assertEquals(id2.getMapping()[2], 0);

  }

}
