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

import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
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
 * Statement node for DDL : drop diskstore
 * 
 * @author ymahajan
 * 
 */
public class DropDiskStoreNode extends DDLStatementNode {

  private String diskStoreName;

  private boolean onlyIfExists;
  
  public DropDiskStoreNode() {
  }

  @Override
  public void init(Object arg1, Object arg2) throws StandardException {
    diskStoreName = ((String)arg1).toUpperCase();
    
    onlyIfExists = ((Boolean)arg2).booleanValue();
  }

  @Override
  public String statementToString() {
    return "DROP DISKSTORE";
  }

  @Override
  public ConstantAction makeConstantAction() {
		return	getGenericConstantActionFactory().getDropDiskStoreConstantAction(
				diskStoreName,onlyIfExists);
  }

  public static void dummy() {
  }
}
