/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.RoutinePrivilegeInfo

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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.AliasDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.RoutinePermsDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementRoutinePermission;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.SYSROUTINEPERMSRowFactory;

import java.util.List;

public class RoutinePrivilegeInfo extends PrivilegeInfo
{
	private AliasDescriptor aliasDescriptor;

	public RoutinePrivilegeInfo( AliasDescriptor aliasDescriptor)
	{
		this.aliasDescriptor = aliasDescriptor;
	}
	
	/**
	 *	This is the guts of the Execution-time logic for GRANT/REVOKE of a routine execute privilege
	 *
	 * @param activation
	 * @param grant true if grant, false if revoke
	 * @param grantees a list of authorization ids (strings)
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void executeGrantRevoke( Activation activation,
									boolean grant,
									List grantees)
		throws StandardException
	{
		// Check that the current user has permission to grant the privileges.
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		String currentUser = lcc.getAuthorizationId();
		TransactionController tc = lcc.getTransactionExecute();

// GemStone changes BEGIN
		dd.startWriting(lcc);
		SchemaDescriptor sd = dd.getSchemaDescriptor(
		    aliasDescriptor.getSchemaUUID(), tc);
		this.schemaName = sd.getSchemaName();
		this.objectName = aliasDescriptor.getObjectName();
// GemStone changes END
		// Check that the current user has permission to grant the privileges.
		checkOwnership( currentUser,
						aliasDescriptor,
// GemStone changes BEGIN
						sd,
						/* (original code)
						dd.getSchemaDescriptor( aliasDescriptor.getSchemaUUID(), tc),
						*/
// GemStone changes END
						dd);
		
		DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

		RoutinePermsDescriptor routinePermsDesc = ddg.newRoutinePermsDescriptor( aliasDescriptor, currentUser);

// GemStone changes BEGIN
		// dd.startWriting(lcc);
		// Add or remove the privileges to/from the SYS.SYSTABLEPERMS and SYS.SYSCOLPERMS tables
		for (GranteeIterator itr = new GranteeIterator(grantees,
		    routinePermsDesc, grant,
		    DataDictionary.SYSROUTINEPERMS_CATALOG_NUM,
		    SYSROUTINEPERMSRowFactory.ALIASID_INDEX_NUM,
		    SYSROUTINEPERMSRowFactory.LDAP_GROUP_NAME,
		    dd, tc); itr.hasNext();)
		/* (original code)
		for( Iterator itr = grantees.iterator(); itr.hasNext();)
		*/
// GemStone changes END
		{
			// Keep track to see if any privileges are revoked by a revoke 
			// statement. If a privilege is not revoked, we need to raise a
			// warning.
			boolean privileges_revoked = false;
// GemStone changes BEGIN
			String grantee = itr.moveNext();
			/* (original code)
			String grantee = (String) itr.next();
			*/
// GemStone changes END
			if (dd.addRemovePermissionsDescriptor( grant, routinePermsDesc, grantee, tc)) 
			{
				privileges_revoked = true;	
				//Derby currently supports only restrict form of revoke execute
				//privilege and that is why, we are sending invalidation action 
				//as REVOKE_PRIVILEGE_RESTRICT rather than REVOKE_PRIVILEGE
				dd.getDependencyManager().invalidateFor
					(routinePermsDesc,
					 DependencyManager.REVOKE_PRIVILEGE_RESTRICT, lcc);

				// When revoking a privilege from a Routine we need to
				// invalidate all GPSs refering to it. But GPSs aren't
				// Dependents of RoutinePermsDescr, but of the
				// AliasDescriptor itself, so we must send
				// INTERNAL_RECOMPILE_REQUEST to the AliasDescriptor's
				// Dependents.
				dd.getDependencyManager().invalidateFor
					(aliasDescriptor,
					 DependencyManager.INTERNAL_RECOMPILE_REQUEST, lcc);
			}
			
			addWarningIfPrivilegeNotRevoked(activation, grant, privileges_revoked, grantee);
		}
	} // end of executeConstantAction
}
