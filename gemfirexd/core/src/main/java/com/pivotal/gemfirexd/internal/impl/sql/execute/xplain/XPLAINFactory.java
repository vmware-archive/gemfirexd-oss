/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINFactory

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.sql.execute.xplain;

import java.sql.SQLException;

import com.pivotal.gemfirexd.internal.catalog.SystemProcedures;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.cohort.OutgoingResultSetImpl;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.ConnectionUtil;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.xplain.XPLAINFactoryIF;
import com.pivotal.gemfirexd.internal.impl.sql.execute.AbstractPolymorphicStatisticsCollector;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BasicNoPutResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.NoRowsResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.StatementPlanCollector;
import com.pivotal.gemfirexd.internal.impl.sql.execute.StatementStatisticsCollector;
import com.pivotal.gemfirexd.internal.impl.sql.execute.TemporaryRowHolderResultSet;

/**
 * This is the module implementation of the XPLAINFactoryIF. It gets lazy-loaded
 * when needed. The factory method determines which visitor to use. The visitor
 * is cached in this factory for later reuse.
 * 
 */
public class XPLAINFactory implements XPLAINFactoryIF {
 
  /** used to neutralize collection during race conditions. */
  private final static ResultSetStatisticsVisitor NO_OP = new NoOpCollector();
  
  /** the last instance of a visitor is cached */
  private ResultSetStatisticsVisitor currentVisitor = null;
  
  /** the current cached schema */
  private String currentSchema = null;
  
  public XPLAINFactory() {
  }

//GemStone changes BEGIN
  /**
   * the factory method, which gets called to determine and return an
   * appropriate XPLAINVisitor instance
   * @throws StandardException 
   */
  public ResultSetStatisticsVisitor getXPLAINVisitor(
      final LanguageConnectionContext lcc, 
      final boolean statsEnabled, 
      final boolean explainConnection) throws StandardException {
    
    ResultSetStatisticsVisitor result = null;

    if (explainConnection) {
      result = new StatementPlanCollector(result);
    }
    
    if (statsEnabled) {
      result = new StatementStatisticsCollector(result);
    }
    
    // making it adaptable instead of assert failure. #43946.
    if (result == null && !lcc.getRunTimeStatisticsModeExplicit()) {
      lcc.setRunTimeStatisticsMode(false, true);
      result = NO_OP;
    }

    /*
    if (SanityManager.ASSERT) {
      SanityManager
          .ASSERT(
              result != null,
              "XPLAINFactory: getXPLAINVisitor should always return a non-null object. statsEnabled="
                  + lcc.statsEnabled()
                  + "; explainConnection="
                  + lcc.getExplainConnection()
                  + "; explainSchema="
                  + lcc.getExplainSchema());
    }*/
    
    if (GemFireXDUtils.TracePlanGeneration) {
      SanityManager.DEBUG_PRINT("fine:"+GfxdConstants.TRACE_PLAN_GENERATION,
          "XPLAINFacory: Returning with statistical visitor : " + result);
    }
    return result == null ? NO_OP : result;
  }

  //GemStone changes END
  
  /**
   * uncache the visitor and reset the factory state
   */
  public void freeResources() {
    // let the garbage collector destroy the visitor and schema
    currentVisitor = null;
    currentSchema = null;
  }

//GemStone changes BEGIN
  @Override
  public final synchronized void applyXPLAINMode(String key, Object value)
      throws StandardException {

    //okay, configure this key.
    if(value != null) {
      
        if(Property.STATEMENT_STATISTICS_MODE.equals(key)) {
          
          //currentVisitor = new StatementStatisticsCollector(currentVisitor);
          constructVisitor(new StatementStatisticsCollector(currentVisitor));
          
        }
        else if (Property.STATEMENT_EXPLAIN_MODE.equals(key)) {
          
          if (currentSchema != null && currentSchema.equals(value.toString())) {
            return;
          }
          
          currentSchema = value.toString();
          
          try {
            SystemProcedures.SET_EXPLAIN_SCHEMA(ConnectionUtil.getCurrentLCC());
          } catch (SQLException e) {
            throw StandardException.unexpectedUserException(e);
          }

          //once we are all set, now lets configure the visitor.
          //currentVisitor = new StatementPlanCollector(currentVisitor);
          constructVisitor(new StatementPlanCollector(currentVisitor));
        }
    }
    //okay, remove this setting.
    else {
      
      if(Property.STATEMENT_STATISTICS_MODE.equals(key)) {
        removeVisitor(StatementStatisticsCollector.class);
      }
      else if (Property.STATEMENT_EXPLAIN_MODE.equals(key)) {
        currentSchema = null;
        try {
          SystemProcedures.SET_EXPLAIN_SCHEMA(ConnectionUtil.getCurrentLCC());
        } catch (SQLException e) {
          throw StandardException.unexpectedUserException(e);
        }
        removeVisitor(StatementPlanCollector.class);
      }
      
    }
  }
  
  private void constructVisitor(ResultSetStatisticsVisitor target) {
    
    ResultSetStatisticsVisitor current = currentVisitor;
    Class<? extends ResultSetStatisticsVisitor> targetClass = target.getClass();
    
    while (current != null && !targetClass.isInstance(current)) {
      current = current.getNextCollector();
    }

    // didn't found the target visitor, so accepting the target.
    if (current == null) {
      currentVisitor = target;
    }
    
    //otherwise discarding the target to configure as already exists.
    
  }
  
  private void removeVisitor(Class<? extends ResultSetStatisticsVisitor> target) {

    ResultSetStatisticsVisitor current = currentVisitor, parent = null;
    
    while (current != null && !target.isInstance(current)) {
      parent = current;
      current = current.getNextCollector();
    }

    //didn't found the target visitor configured.
    if(current == null) {
      return;
    }

    if(parent != null) {
      parent.setNextCollector(current.getNextCollector());
    }
    else {
      currentVisitor = current.getNextCollector();
    }

  }

  /**
   * This is primarily there to avoid NPE checks every calling point of getXPlainVisitor().
   * 
   * @author soubhikc
   *
   */
  private static class NoOpCollector extends AbstractPolymorphicStatisticsCollector {
    @Override
    public void visitVirtual(
        NoRowsResultSetImpl rs) {
    }

    @Override
    public void visitVirtual(
        BasicNoPutResultSetImpl rs) {
    }

    @Override
    public void visitVirtual(
        AbstractGemFireResultSet rs) {
    }

    @Override
    public void visitVirtual(
        OutgoingResultSetImpl rs) {
    }

    @Override
    public void visitVirtual(
        TemporaryRowHolderResultSet rs) {
    }
    
    @Override
    public Object clone() {
      return this;
    }
    
    @Override
    final public ResultSetStatisticsVisitor getClone() {
      return this;
    }

  }
//GemStone changes END

}
