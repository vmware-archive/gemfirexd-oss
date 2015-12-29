/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GrantRevokeConstantAction

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

package com.pivotal.gemfirexd.internal.impl.sql.execute;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

import java.util.List;

//GemStone changes BEGIN
class GrantRevokeConstantAction extends DDLConstantAction
/*class GrantRevokeConstantAction implements ConstantAction*/
//GemStone changes END
{
	private boolean grant;
	private PrivilegeInfo privileges;
	private List grantees;

	GrantRevokeConstantAction( boolean grant,
							   PrivilegeInfo privileges,
							   List grantees)
	{
		this.grant = grant;
		this.privileges = privileges;
		this.grantees = grantees;
	}

	public	String	toString()
	{
		return grant ? "GRANT" : "REVOKE";
	}


	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE
	 *
	 *	See ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		privileges.executeGrantRevoke( activation, grant, grantees);
	}

// GemStone changes BEGIN
  @Override
  public String getSchemaName() {
    return this.privileges.getSchemaName();
  }

  @Override
  public String getTableName() {
    return this.privileges.getTableName();
  }

  @Override
  public final String getObjectName() {
    return this.privileges.getObjectName();
  }

  @Override
  public boolean isCancellable() {
    return false;
  }
// GemStone changes END
}
