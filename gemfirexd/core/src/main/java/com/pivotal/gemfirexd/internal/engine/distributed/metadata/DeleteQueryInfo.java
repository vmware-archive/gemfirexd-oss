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

import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.impl.sql.compile.AndNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.DeleteNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.FromBaseTable;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ProjectRestrictNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ResultColumn;

public class DeleteQueryInfo extends DMLQueryInfo {

  private boolean isDeleteWithReferencedKeys;
  private int deleteTargetTableNum = -1;
  public DeleteQueryInfo(QueryInfoContext qic) throws StandardException {
    super(qic);
    qic.setRootQueryInfo(this);
    //By default assume that it is primary key based & we will negate it later in the init
    this.queryType = GemFireXDUtils.set(this.queryType, IS_PRIMARY_KEY_TYPE);
  }

  @Override
  public void init() throws StandardException {
    super.init();
//    Iterator<PredicateList>  itr = this.wherePredicateList.iterator();
//    while(itr.hasNext()) {
//      this.handlePredicateList(itr.next());
//      itr.remove();
//    } 
    TableQueryInfo tqi = null;
    // Is Convertible to get
    if (this.tableQueryInfoList.size() != 1
        || !(tqi = this.tableQueryInfoList.get(0)).isPrimaryKeyBased()
        || !isWhereClauseSatisfactory(tqi) ) {
      this.queryType = GemFireXDUtils
          .clear(this.queryType, IS_PRIMARY_KEY_TYPE);
    }
   
    final GemFireContainer container = (GemFireContainer)getTargetRegion()
        .getUserAttribute();
    if(!container.isTemporaryContainer()) {
      this.isDeleteWithReferencedKeys = container.getExtraTableInfo()
      .getReferencedKeyColumns() != null;
    }
    else {
      this.isDeleteWithReferencedKeys = false;
    }
  }

  @Override
  public Visitable visit(Visitable node) throws StandardException {
    if( node instanceof DeleteNode) {
      DeleteNode dn = (DeleteNode)node;
      this.deleteTargetTableNum = dn.getTargetTableID();
      this.visit(dn.getResultSetNode());      
      
    }
    else if(node instanceof ProjectRestrictNode) {
      this.handleProjectRestrictNode((ProjectRestrictNode)node);      
    }else {
      super.visit(node);
    }
    return node;
  }

  public boolean skipChildren(Visitable node) throws StandardException {    
    return node instanceof ProjectRestrictNode || node instanceof FromBaseTable
        || node instanceof AndNode || node instanceof ResultColumn;
  }

  public boolean stopTraversal() {
    // TODO Auto-generated method stub
    return false;
  }  

  @Override
  public final boolean isDelete() {
    return true;
  }

  public final boolean isDeleteWithReferencedKeys() {
    return this.isDeleteWithReferencedKeys;
  }
  
  public LocalRegion getTargetRegion() {
    return this.tableQueryInfoList.get(this.deleteTargetTableNum).getRegion();
  }

//  @Override
//  public LocalRegion getRegion() {
//    return this.tableQueryInfoList.get(0).getRegion();
//  }
}
