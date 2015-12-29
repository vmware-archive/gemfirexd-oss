/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementRoutinePermission

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

package com.pivotal.gemfirexd.internal.iapi.sql.dictionary;

import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.RoutinePermsDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;

/**
 * This class describes a routine execute permission
 * required by a statement.
 */

public final class StatementRoutinePermission extends StatementPermission
{
	private UUID routineUUID;

	public StatementRoutinePermission( UUID routineUUID)
	{
		this.routineUUID = routineUUID;
	}
									 
	/**
	 * @see StatementPermission#check
	 */
	public void check( LanguageConnectionContext lcc,
					   String authorizationId,
					   boolean forGrant) throws StandardException
	{
		DataDictionary dd = lcc.getDataDictionary();
		TransactionController tc = lcc.getTransactionExecute();
		
// GemStone changes BEGIN
		// bypass permissions for some special routines like CHANGE_PASSWORD here (#48372)
		// the requisite checks will be done explicitly in the method call itself
		if (!dd.bypassRoutineAuthorization(routineUUID)) {
		  check(routineUUID, authorizationId, forGrant, dd, tc);
		}
	}

	public static void check(UUID routineUUID, String authorizationId,
	    boolean forGrant, DataDictionary dd, TransactionController tc)
	    throws StandardException {
// GemStone changes END
		RoutinePermsDescriptor perms = dd.getRoutinePermissions( routineUUID, authorizationId);
		if( perms == null || ! perms.getHasExecutePermission())
			perms = dd.getRoutinePermissions(routineUUID, Authorizer.PUBLIC_AUTHORIZATION_ID);

		if( perms == null || ! perms.getHasExecutePermission())
		{
			AliasDescriptor ad = dd.getAliasDescriptor( routineUUID);
			if( ad == null)
				throw StandardException.newException( SQLState.AUTH_INTERNAL_BAD_UUID, "routine");
			SchemaDescriptor sd = dd.getSchemaDescriptor( ad.getSchemaUUID(), tc);
			if( sd == null)
				throw StandardException.newException( SQLState.AUTH_INTERNAL_BAD_UUID, "schema");
			throw StandardException.newException( forGrant ? SQLState.AUTH_NO_EXECUTE_PERMISSION_FOR_GRANT
												  : SQLState.AUTH_NO_EXECUTE_PERMISSION,
												  authorizationId,
												  ad.getDescriptorType(),
												  "",
												  sd.getSchemaName(),
												  ad.getDescriptorName());
		}
	} // end of check

	/**
	 * @see StatementPermission#getPermissionDescriptor
	 */
	public PermissionsDescriptor getPermissionDescriptor(String authid, DataDictionary dd)
	throws StandardException
	{
		return dd.getRoutinePermissions(routineUUID,authid);
	}
}
