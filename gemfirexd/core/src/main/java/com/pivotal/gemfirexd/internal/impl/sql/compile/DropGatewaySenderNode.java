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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * Statement Node for DROP GATEWAYSENDER DDL
 * 
 * @author Yogesh Mahajan
 * @since 1.0
 * 
 */
public class DropGatewaySenderNode extends DDLStatementNode {

  private String id;
  private Boolean onlyIfExists;

  public DropGatewaySenderNode() {
  }

  @Override
  public void init(Object arg1, Object arg2) throws StandardException {
    this.id = (String)arg1;
    this.onlyIfExists = ((Boolean)arg2).booleanValue();
  }

  @Override
  public String statementToString() {
    return "DROP GATEWAYSENDER";
  }

  @Override
  public ConstantAction makeConstantAction() {
	  return getGenericConstantActionFactory().getDropGatewaySenderConstantAction(
			  id,onlyIfExists);
  }

  public static void dummy() {
  }
}
