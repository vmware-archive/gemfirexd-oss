
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
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
 * Changes for GemFireXD distributed data platform.
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

package com.pivotal.gemfirexd.internal.engine.ddl;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.impl.sql.compile.CreateSchemaNode;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;
import com.pivotal.gemfirexd.internal.impl.sql.execute.CreateSchemaConstantAction;
import com.pivotal.gemfirexd.internal.impl.sql.execute.GenericConstantActionFactory;

/**
 * An GfxdCreateSchemaNode is the root of a QueryTree that represents a CREATE
 * SCHEMA statement with GemFireXD extensions.
 * 
 * Overall organization of new code for DDL extensions w.r.t. Derby is as below.
 * The basic idea is to allow for future Derby additions as easily as possible.
 * We avoid changing the constructors of {@link ConstantAction} implementations
 * since they are used elsewhere too (e.g.
 * DDLConstantAction.getSchemaDescriptorForCreate). Instead of adding new
 * constructors to {@link CreateSchemaConstantAction} class we provide setters
 * for GemFireXD additions. Similar is done for {@link TupleDescriptor}
 * implementations.
 * 
 * The {@link GenericConstantActionFactory} is retained as such without any
 * changes and the {@link StatementNode} implementations are extended to add
 * initialization for the GemFireXD additions. The
 * {@link StatementNode#makeConstantAction} method implementation calls the base
 * class method and then invokes the setters for the GemFireXD additions.
 * 
 * @author Sumedh Wale
 * @since 6.0
 */
public class GfxdCreateSchemaNode extends CreateSchemaNode {

  private ServerGroupsTableAttribute defaultSG;

  /**
   * Initializer for an GfxdCreateSchemaNode
   * 
   * @param schemaName
   *          The name of the new schema
   * @param aid
   *          The authorization id
   * @param defaultSG
   *          The name of the server group to be used as default for TABLEs that
   *          don't specify one explicitly
   * 
   * @exception StandardException
   *              Thrown on error
   */
  @Override
  public void init(Object schemaName, Object aid, Object defaultSG)
      throws StandardException {
    super.init(schemaName, aid);
    this.defaultSG = (ServerGroupsTableAttribute)defaultSG;
  }

  /**
   * Convert this object to a String. See comments in QueryTreeNode.java for how
   * this should be done for tree printing.
   * 
   * @return This object as a String
   */
  @Override
  public String toString() {
    if (SanityManager.DEBUG) {
      return super.toString() + "defaultServerGroup: " + "\n" + this.defaultSG
          + "\n";
    }
    else {
      return "";
    }
  }

  /**
   * Create the Constant information that will drive the guts of Execution.
   * 
   * @exception StandardException
   *              Thrown on failure
   */
  @Override
  public ConstantAction makeConstantAction() {
    CreateSchemaConstantAction action = (CreateSchemaConstantAction)super
        .makeConstantAction();
    action.setDefaultSG(this.defaultSG);
    return action;
  }

}
