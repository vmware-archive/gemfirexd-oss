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
package com.pivotal.gemfirexd.internal.engine.distributed.metadata;

import java.util.ArrayList;
import java.util.ListIterator;
import java.util.Stack;

import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TIntHashSet;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizer;
import com.pivotal.gemfirexd.internal.impl.sql.compile.OptimizerImpl;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ProjectRestrictNode;

/**
 * An helper class which will allow certain states of the nodes
 * to be collected when traversing the tree   
 * @since GemFireXD
 * @author Asif
 *
 *
 */
public final class QueryInfoContext { 
  private final int paramCount;
  private final  Stack<Scope> scopes;
  private DMLQueryInfo rootQueryInfo;
  //The root select will have scope  level of 0
  private int scopeLevel = -1;
  private final int absoluteStart ;
  private String virtualTable;
  private boolean isVTIDistributable;
  private byte flags;
  public static final byte PREP_STATEMENT = 0x01;
  public static final byte HAS_DSID = 0x02;
  public static final byte CREATE_QUERY_INFO = 0x04;
  public static final byte IS_ORIGINALLY_SUBQUERY = 0x10;
  public static final byte WITH_SECONDARIES = 0x20;
  public static final byte DISABLE_COLOCATION_CHECK = 0x40;

  
  private final byte[] querySectionForParameter;
  private byte currentQuerySection;

  /**
   * we shouldn't expose the handle of optimizer outside the context to the
   * extent possible, so that inadvertently we don't hold onto resources in
   * QueryInfo which stays beyond query plan generation phase.
   * 
   * Have individual methods like 
   */
  private OptimizerImpl optimizer;
  
  public QueryInfoContext(boolean createQueryInfo, int paramCount, boolean isPrepStatement)
      throws StandardException {
    this.flags = GemFireXDUtils.set(flags, CREATE_QUERY_INFO, createQueryInfo);
    this.flags = GemFireXDUtils.set(flags, PREP_STATEMENT, isPrepStatement);
    this.paramCount = paramCount;
    this.scopes = new Stack<Scope>();
    this.absoluteStart = 0;
    this.querySectionForParameter = new byte[this.paramCount];
    this.currentQuerySection=  DMLQueryInfo.DEFAULT_DML_SECTION;
  }

  public QueryInfoContext(boolean createQueryInfo, int paramCount,
      int absoluteStart, boolean isPrepStatement) throws StandardException {
    this.flags = GemFireXDUtils.set(flags, CREATE_QUERY_INFO, createQueryInfo);
    this.flags = GemFireXDUtils.set(flags, PREP_STATEMENT, isPrepStatement);
    this.paramCount = paramCount;
    this.scopes = new Stack<Scope>();
    this.absoluteStart = absoluteStart;
    this.querySectionForParameter = new byte[this.paramCount];
    this.currentQuerySection=  DMLQueryInfo.DEFAULT_DML_SECTION;
  }

  public int getParamerCount() {
    return this.paramCount;
  }
  void pushScope(DMLQueryInfo top)  {   
   this.scopes.push(new Scope(top,++scopeLevel));
  }
  
  public Scope popScope() {    
    Scope poppedScope = this.scopes.pop();
    --this.scopeLevel;
    assert (this.scopeLevel ==0 && scopes.isEmpty()) || !scopes.isEmpty();
    if(poppedScope.getScopeLevel() > 0) {
      //This is a subquery, lets make the independent PR based scope to -1 so that it will never refer 
      // to wrong scope
      ((SubQueryInfo)poppedScope.getDMLQueryInfo()).cleanUp();
    }
    poppedScope.unsetLevel();
    return poppedScope;
  }

  public void setRootQueryInfo(DMLQueryInfo root) {
    this.rootQueryInfo = root;
  }
  
  public DMLQueryInfo  getRootQueryInfo() {
    return this.rootQueryInfo; 
  }
  
  
  public final boolean createQueryInfo() {
    return GemFireXDUtils.isSet(flags, CREATE_QUERY_INFO);
  }  
  public void addToPRTableCount(int cnt) {    
    this.scopes.peek().addToPRTableCount(cnt);    
  }
 
  DMLQueryInfo getLastPushedIndependentQueryInfo() {
    assert this.scopes.size() > 1;
    //skip itself
    ListIterator<Scope> scopeItr = this.scopes.listIterator(this.scopeLevel);
    DMLQueryInfo found= null;
    while(scopeItr.hasPrevious()) {
      Scope scope = scopeItr.previous();
      DMLQueryInfo dqi = scope.getDMLQueryInfo();      
      if(dqi.isSubQueryInfo()) {
          if(((SubQueryInfo)dqi).isIndependent()) {
            found = dqi;        
            break;
          }
      }else {
        //Either it is the topmost query or is independent subquery. Either way it will be independent
       //Check if it is PR based . If yes, we have got what we wanted, else need to go further up
        //There will be no problem as far as presence of dummy tablequeryinfo at 0th index is concerned,
        // because that is added only to correlated subquery and not for independent queries.
        //And for correlated subquery we do not rely on the driver region's partitioning policy
        //but the independentScope  count which is contained in the field.
        found = dqi;
        break;
      }        
    }
    return found;
  }
  public void addToTableCount(int cnt) {
    this.scopes.peek().addToTableCount(cnt);    
  }

  public int getPRTableCount() {   
     return this.scopes.peek().getPRTableCount();
   
  }

  public int getTableCount() {
      return this.scopes.peek().getTableCount();    
  } 
  
  void setParentPRN(ProjectRestrictNode prn) {
    this.scopes.peek().setParentPRN(prn);    
  }
  
  public ProjectRestrictNode getParentPRN() {
    return this.scopes.peek().getParentPRN();
  }

  /**
   * The caller should call this method to release any 
   * unwarranted handles to the query tree. 
   * 
   * If not done so, parentPRN like references will leak
   * memory.
   */
  public void cleanUp() {
    this.scopes.peek().setParentPRN(null);    
  }
  
  int getNestingLevelOfScope() {
    return this.scopes.size()-1;
  }
  
  DMLQueryInfo getCurrentScopeQueryInfo() {
    return this.scopes.peek().getDMLQueryInfo();
  }
  
  //actual table number is 0 based index 
  Scope findOwningScope(int actualTableNumber) {
    //special handling so as to avoid picking up the TableQueryInfo at 0th index
    //  in case the dml being investigated is a subquery . without this check
    // the column will get associated with wrong Scope.
    if(actualTableNumber == 0) {
      return this.getScopeAt(0);
    }
    //Do not check in the current scope
    ListIterator<Scope> scopeItr = this.scopes.listIterator(this.scopeLevel);
    Scope found = null;
    while(scopeItr.hasPrevious()) {
      Scope scope = scopeItr.previous();
      DMLQueryInfo dqi = scope.getDMLQueryInfo();      
      if(dqi.getTableQueryInfo(actualTableNumber) != null) {
       found = scope;
       break;
      }
    }
    return found;    
  }
  
  int getLastPushedPRBasedIndependentScope() {
    assert this.scopes.size() > 1;
    //skip itself
    ListIterator<Scope> scopeItr = this.scopes.listIterator(this.scopeLevel);
    int found = -1;
    while(scopeItr.hasPrevious()) {
      Scope scope = scopeItr.previous();
      DMLQueryInfo dqi = scope.getDMLQueryInfo();      
      //In case the subquery(q0) is dependent on an outer query, such that 
      //the outer query  has following properties:
      // The outer query(q1) is PR based and is itself a dependent query on outer - outer (q2) .
      //If query(q2) happens to be replicated based, then the q1 , though dependent can still be
      // considered as PR based independent query , as far as q0 is concerned
      //Therefore instead of previous check of (SubQueryInfo)dqi).isCorrelated(), we should check
      // if subquery has prbased indpndnt scope > -1 use it else consider it to be independent for 
      // our purpose. 
      //

      int subqueryPRScopeLevel = -1;
      if(dqi.isSubQueryInfo() && (subqueryPRScopeLevel =
        ((SubQueryInfo)dqi).getLastPushedPRBasedIndependentScopeLevel()) != -1 ) {
        found = subqueryPRScopeLevel;        
        break;
      }else {
        //Either it is the topmost query or is independent subquery. Either way it will be independent
       //Check if it is PR based . If yes, we have got what we wanted, else need to go further up
        //There will be no problem as far as presence of dummy tablequeryinfo at 0th index is concerned,
        // because that is added only to correlated subquery and not for independent queries.
        //And for correlated subquery we do not rely on the driver region's partitioning policy
        //but the independentScope  count which is contained in the field.
        if(dqi.getRegion().getDataPolicy().withPartitioning()) {
          found = scope.getScopeLevel();
          break;
        }
      }        
    }
    return found;  
  }
  
  Scope getScopeAt(int index) {
    return this.scopes.elementAt(index);
  }

  public int getAbsoluteStart() {
    return this.absoluteStart;
  }

  public final boolean optimizeForWrite() {
    return GemFireXDUtils.isSet(flags, HAS_DSID);
  }
  
  public boolean withSecondaries() {
    return GemFireXDUtils.isSet(flags, WITH_SECONDARIES);
  }

  public final void setOptimizeForWrite(boolean flag) {
    this.flags = GemFireXDUtils.set(flags, HAS_DSID, flag);
  }

  public final void setWithSecondaries(boolean flag) {
    this.flags = GemFireXDUtils.set(flags, WITH_SECONDARIES, flag);
  }

  public final void setOptimizer(Optimizer opt) {
    this.optimizer = (OptimizerImpl)opt;
    // TODO - Disable for now
    // this.flags = GemFireXDUtils.set(flags, DISABLE_COLOCATION_CHECK,
    // this.optimizer.hasNonColocatedTables());
  }
  
  public final boolean isColocationCheckDisabled() {
    return GemFireXDUtils.isSet(flags, DISABLE_COLOCATION_CHECK);
  }

  public final ArrayList<THashSet> getTableColocationPrCorrName() {
    return optimizer.getBestJoinOrderColocationPrCorrName();
  }

  public final int getDriverTablePrID() {
    return optimizer.getBestJoinOrderDriverTablePrID();
  }
  
  public final String virtualTable() {
    return this.virtualTable;
  }

  public final boolean isVTIDistributable() {
    return this.isVTIDistributable;
  }

  public final void setHasVirtualTable(String tableName,
      boolean isVTIDistributable) {
    // don't overwrite if current VTI was distributable but incoming is not
    if (isVTIDistributable || !this.isVTIDistributable) {
      this.virtualTable = tableName;
      this.isVTIDistributable = isVTIDistributable;
    }
  }

  public boolean isPreparedStatementQuery() {
    return GemFireXDUtils.isSet(flags, PREP_STATEMENT);
  }
  
  void setQuerySectionUnderAnalysis(byte querySection) {
    this.currentQuerySection = querySection;
  }
  
  public void foundParameter(int paramNumber) {
    this.querySectionForParameter[paramNumber] = this.currentQuerySection;
  }
  
  byte getQuerySectionForParameter(int paramNumber) {
    return this.querySectionForParameter[paramNumber] ;
        
  }
}
