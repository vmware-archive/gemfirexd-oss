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

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.operations.DiskStoreCreateOperation;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore.VMKind;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.impl.sql.execute.DDLConstantAction;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author asif, ymahajan
 * 
 */
public final class CreateDiskStoreNode extends DDLStatementNode {

  private String diskStoreName;

  private List<String> dirPaths;

  private List<Integer> dirSizes;

  private Map otherAttribs;

  public CreateDiskStoreNode() {
  }

  @Override
  public void init(Object arg1, Object arg2, Object arg3, Object arg4)
      throws StandardException {
    // only create disk stores on data store nodes
    diskStoreName = SharedUtils.SQLToUpperCase((String)arg1);
    dirPaths = (List<String>)arg2;
    dirSizes = (List<Integer>)arg3;
    otherAttribs = (Map)arg4;
  }

  @Override
  public String statementToString() {
    return "CREATE DISKSTORE";
  }

  @Override
  public ConstantAction makeConstantAction() {
		return	getGenericConstantActionFactory().getCreateDiskStoreConstantAction(
				diskStoreName, dirPaths, dirSizes, otherAttribs);
  }
}
