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
package com.pivotal.gemfirexd.internal.impl.sql.compile;

import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DDLConstantAction;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * Statement Node for DROP ASYNCEVENTLISTENER DDL
 * 
 * @author Yogesh Mahajan
 * @since 1.0
 * 
 */
public class DropAsyncEventListenerNode extends DDLStatementNode {

  private String id;

  private boolean onlyIfExists;
  
  public DropAsyncEventListenerNode() {
  }

  @Override
  public void init(Object arg1, Object arg2) throws StandardException {
    this.id = (String)arg1;
    this.onlyIfExists = ((Boolean)arg2).booleanValue();
  }

  //TODO : implement bindStatement() to check object existence before execution
  
  @Override
  public String statementToString() {
    return "DROP ASYNCEVENTLISTENER";
  }

  // Create a DROP ASYNCEVENTLISTENER constant action class, called at execute time to
  // modify catalog and stop Gemfire cache objects
  @Override
  public ConstantAction makeConstantAction() {
		return	getGenericConstantActionFactory().getDropAsyncEventListenerConstantAction(
				id,onlyIfExists);
  }

  
  public static void dummy() {
  }
}
