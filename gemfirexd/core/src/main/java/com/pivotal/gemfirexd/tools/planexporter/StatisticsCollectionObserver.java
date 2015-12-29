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
package com.pivotal.gemfirexd.tools.planexporter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.pivotal.gemfirexd.internal.engine.distributed.message.StatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.XPLAINDistPropsDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINResultSetDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINScanPropsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINSortPropsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINStatementDescriptor;

public class StatisticsCollectionObserver implements Serializable {
  private static final long serialVersionUID = 214366045626682250L;

  private static StatisticsCollectionObserver _theInstance = null;

  public void observeXMLData(TreeNode[] dataArr) {
  }

  public void reset() {
  }

  public <T extends Serializable> void processSelectMessage(
      XPLAINStatementDescriptor stmt, ArrayList<XPLAINResultSetDescriptor> rsets,
      List<XPLAINResultSetTimingsDescriptor> rsetsTimings,
      List<XPLAINScanPropsDescriptor> scanProps,
      List<XPLAINSortPropsDescriptor> sortProps,
      List<XPLAINDistPropsDescriptor> disProps) {
  }

  public <T extends Serializable> void processCUDMessage(
      final StatementExecutorMessage<T> msg) {
  }

  public <T extends Serializable> void processResultSetMessage(
      final ResultSet rs) {
  }

  public <T extends Serializable> void processDistributionMessage(
      final StatementExecutorMessage<T> msg) {
  }

  public void processedResultSetDescriptor(final XPLAINResultSetDescriptor rdesc) {
  }

  public void processedResultSetTimingDescriptor(
      final XPLAINResultSetTimingsDescriptor rdescT) {
  }
  
  public void processedScanPropsDescriptor(
      final XPLAINScanPropsDescriptor scanP) {
  }

  public void processedSortPropsDescriptor(
      final XPLAINSortPropsDescriptor sortP) {
  }
  
  public void processedDistPropsDescriptor(
      final XPLAINDistPropsDescriptor distP) {
  }

  public final static StatisticsCollectionObserver getInstance() {
    return _theInstance;
  }

  public final synchronized static StatisticsCollectionObserver setInstance(
      StatisticsCollectionObserver theObserver) {
    _theInstance = theObserver;
    return _theInstance;
  }

  public void end() {
  }
}
