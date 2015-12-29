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
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import com.pivotal.gemfirexd.internal.engine.distributed.ResultHolder;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.GroupedIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.OrderedIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.RSIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.RoundRobinIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.RowCountIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.SequentialIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.SetOperatorIterator;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet.SpecialCaseOuterJoinIterator;

/**
 * Specialized visitor for GemFireDistributedResultSet iterations.
 * 
 * @author soubhikc
 *
 */
public interface IteratorStatisticsVisitor extends GfxdStatisticsVisitor {

  public void setNumberOfChildren(
      int noChildren);
  
  public void visit(
      RSIterator it,
      int donotHonor);

  public void visit(
      SequentialIterator it);

  public void visit(
      RoundRobinIterator it);

  public void visit(
      OrderedIterator it);

  public void visit(
      GroupedIterator it);

  public void visit(
      SpecialCaseOuterJoinIterator it);

  public void visit(
      RowCountIterator it);
  
  public void visit(
      ResultHolder rh);

  public void visit(SetOperatorIterator setOperatorIterator);
}
