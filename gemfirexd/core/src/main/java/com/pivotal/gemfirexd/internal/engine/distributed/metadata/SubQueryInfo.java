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

import java.util.Set;
import java.util.SortedSet;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * 
 * @author Asif
 *
 */
public class SubQueryInfo extends SelectQueryInfo
{
  final private static byte NON_CORRELATED_SUBQUERY = 0x00;
  final private static byte SUPPORTED_CORRELATED_SUBQUERY = 0x01;
  final private static byte UNSUPPORTED_CORRELATED_SUBQUERY = 0x02;  
  private byte subqueryDataFlag = NON_CORRELATED_SUBQUERY;
  private final String subqueryStr ;
  //It is to be noted that once the scope is popped , this number will no 
  //longer have any meaning so we should make use of it as long as scope 
  //  is there. Proper asserts need to be in place
  private int prBasedIndependentScopeLevel =-1;
  public SubQueryInfo( QueryInfoContext qic,String subqueryStr, boolean hasCorrelatedCols) throws StandardException {
    super(qic);    
    this.subqueryStr = subqueryStr;
    if(hasCorrelatedCols) {
      //Find out the last pushed independent Scope, if correlated
      this.prBasedIndependentScopeLevel  = qic.getLastPushedPRBasedIndependentScope();
      //If the subquery has  correlated Crs by default assume it to be Unsupported till found otherwise
      // A correlated subquery could be supported without worrying about colocation , if all the tables in subquery
      // are replicated . Though the caveat will be the server groups related which will be tackled later.
      //But at this point we do not know if all the tables of this subquery are replicated, so assume that
      // there  is atleast one PR based table.  So we increment the PR table count and also add the TableQueryInfo of the 
      // driver table of PR based independent query at the relevant index in the TableQueryInfo List so that colocation 
      // criteria is correctly created.
      //Later if we find in the init that it is all replicated tables, we will invalidate colocation criteria.
      this.subqueryDataFlag = GemFireXDUtils.set(subqueryDataFlag, UNSUPPORTED_CORRELATED_SUBQUERY);
      //This indicates the first PR table of the Scope which we found on which this query is should be ultimately dependent on.
      if(this.prBasedIndependentScopeLevel != -1) {        
        DMLQueryInfo temp = qic.getScopeAt(this.prBasedIndependentScopeLevel).getDMLQueryInfo();
        TableQueryInfo masterTQI = temp.getDriverTableQueryInfo();
        assert masterTQI.isPartitionedRegion();
        //Insert a TableQueryInfo corresponding to it. assume that the table number is 0 , so that we can place it at the 
        // 0th index of the list which should be fine, as except for the root query no other table can be  placed at 0th index
        // of table list. Also in the QueryInfoContex while finding the owning Scope for a column , will do special check
        // for table number 0 , because that would mean outermost scope's TableQueryInfo at 0th index.
        TableQueryInfo master = new TableQueryInfo(masterTQI,qic);
        this.tableQueryInfoList.add(0, master);
        this.updateColocationMatrixData(master);
        
      }
    }
    else {
      //This is an independent subquery. Check the nesting level here. If size > 2, 
      //we throw exception. Since the netsing level is zero based , if the count is =2,
      // it implies this is 3 rd scope , we throw exception
      if (qic.getNestingLevelOfScope() == 2) {
        throw StandardException
            .newException(SQLState.SUBQUERY_MORE_THAN_1_NESTING_NOT_SUPPORTED);
      }
    }
  }

 /* void setCorrelatedSubqueryFlag() {
    this.subqueryDataFlag = GemFireXDUtils.set(this.subqueryDataFlag,
        CORRELATED_SUBQUERY);
  }*/

  @Override
  public void computeNodes(Set<Object> routingKeys, Activation activation, boolean forSingleHopPreparePhase)
      throws StandardException {
    QueryInfoConstants.NON_PRUNABLE.computeNodes(routingKeys, activation, false);    
  }
  
  boolean isCorrelated() {
   return GemFireXDUtils.isSet(this.subqueryDataFlag, SUPPORTED_CORRELATED_SUBQUERY) ||
   GemFireXDUtils.isSet(this.subqueryDataFlag, UNSUPPORTED_CORRELATED_SUBQUERY); 
  }
  boolean isIndependent() {
    return GemFireXDUtils.isSet(this.subqueryDataFlag,NON_CORRELATED_SUBQUERY ); 
   }
 
  @Override
  public void init() throws StandardException {
    super.init();
    this.needGfxdSubactivation = (this.isPrimaryKeyBased() || (this
        .getPRTableCount() > 0 && !this.isCorrelated()))
        &&
        // skip setting this flag for loner VM case
        !(Misc.getDistributedSystem().isLoner() && !getTestFlagIgnoreSingleVMCriteria());
    
    //We are here and that too without exception so set the flag to supported correlated subquery , 
    //if it is unsupported correlated subquery
    if(this.isCorrelated()) {
      // If the subquery is PR based , but the outer scope independent PR is -1,
      // implying that the outer scope query on which it is dependent is a
      // replicated table, than at this point we will not support it as
      // executing such query will cause duplicate rows of the outer table to
      // arrive.
      if (this.getRegion().getDataPolicy().withPartitioning()
          && this.prBasedIndependentScopeLevel == -1) {
        throw StandardException
            .newException(SQLState.REPLICATED_PR_CORRELATED_UNSUPPORTED);
      }

      this.subqueryDataFlag = GemFireXDUtils.set(this.subqueryDataFlag,SUPPORTED_CORRELATED_SUBQUERY);
      //Add this subquery to the independent query of which this is the subquery only if the independent scope is 
      //PR based. If the independent scope has all replicated tables, ignore
      if( this.prBasedIndependentScopeLevel != -1) {
        qic.getScopeAt(this.prBasedIndependentScopeLevel).getDMLQueryInfo().addSubQueryInfo(this);
      }else {
        this.checkServerGroupCompatibility();
      }
    }else {
      //This is an independent subquery. If it does not contain any PR , then the replicated
      // tables server groups need to be a super set of the server group of the driver table
      // of its that predecessor which is completely independent
      checkServerGroupCompatibility();
      
      //Add it to the root to correctly pass the gfxd subactivation flag.
      //TODO: Asif: Tackle the case of two levels of independent subqueries as 
      //we are just passing a single flag
      
      qic.getRootQueryInfo().addSubQueryInfo(this);
    }
   
  }

  private void checkServerGroupCompatibility() throws StandardException {
    if (this.qic.getPRTableCount() == 0) {
      // Get the SelectQueryInfo of first independent predecessor
      DMLQueryInfo prev =qic.getLastPushedIndependentQueryInfo();
      SortedSet<String> replicatedSGs = getServerGroups(this
          .getDriverTableQueryInfo());
      if (replicatedSGs != null && replicatedSGs != GemFireXDUtils.SET_MAX) {
        SortedSet<String> predecessor = getServerGroups(prev
            .getDriverTableQueryInfo());
        if (GemFireXDUtils.setCompare(replicatedSGs, predecessor) >= 0) {
          // OK
        }
        else {
          throw StandardException.newException(SQLState.NOT_COLOCATED_WITH,
              prev.getDriverTableQueryInfo().getFullTableName()
                  + "[server groups:" + SharedUtils.toCSV(predecessor) + "]",
              this.getDriverTableQueryInfo().getFullTableName()
                  + "[server groups:" + SharedUtils.toCSV(replicatedSGs) + "]");
        }
      }
    }
  }

  public String getSubqueryString() {
    return this.subqueryStr;
  }
  /*
  @Override
  void handleProjectRestrictNode(ProjectRestrictNode prn)
  throws StandardException {   
    this.processProjectRestrictNode(prn);
  }*/
  
  @Override
  public boolean isSubQueryInfo() {
    return true;
  }
  
  int getLastPushedPRBasedIndependentScopeLevel() {
    return this.prBasedIndependentScopeLevel;
  }
  
  public void cleanUp() {
    this.prBasedIndependentScopeLevel = -1;
  }

}
