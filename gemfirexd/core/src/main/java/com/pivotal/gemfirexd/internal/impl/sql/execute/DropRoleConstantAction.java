/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropRoleConstantAction

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
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.RoleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 *  This class  describes actions that are ALWAYS performed for a
 *  DROP ROLE Statement at Execution time.
 *
 */

class DropRoleConstantAction extends DDLConstantAction
{


    private final String roleName;


    // CONSTRUCTORS

    /**
     *  Make the ConstantAction for a DROP ROLE statement.
     *
     *  @param  roleName  role name to be dropped
     *
     */
    DropRoleConstantAction(String roleName)
    {
        this.roleName = roleName;
    }

    ///////////////////////////////////////////////
    //
    // OBJECT SHADOWS
    //
    ///////////////////////////////////////////////

    public String toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.
        return "DROP ROLE " + roleName;
    }

    // INTERFACE METHODS


    /**
     *  This is the guts of the Execution-time logic for DROP ROLE.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction( Activation activation )
        throws StandardException
    {
        LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();

        /*
        ** Inform the data dictionary that we are about to write to it.
        ** There are several calls to data dictionary "get" methods here
        ** that might be done in "read" mode in the data dictionary, but
        ** it seemed safer to do this whole operation in "write" mode.
        **
        ** We tell the data dictionary we're done writing at the end of
        ** the transaction.
        */
        dd.startWriting(lcc);

        RoleDescriptor rd = dd.getRoleDefinitionDescriptor(roleName);

        if (rd == null) {
            throw StandardException.newException(
                SQLState.ROLE_INVALID_SPECIFICATION, roleName);
        }

        rd.drop(lcc);

        /*
         * We dropped a role, now drop all dependents:
         * - role grants to this role
         * - grants of this role to other roles or users
         * - privilege grants to this role
         */

        dd.dropRoleGrantsByGrantee(roleName, tc);
        dd.dropRoleGrantsByName(roleName, tc);
        dd.dropAllPermsByGrantee(roleName, tc);
    }

// GemStone changes BEGIN

  @Override
  public String getSchemaName() {
    return null;
  }

  @Override
  public boolean isCancellable() {
    return false;
  }

// GemStone changes END    
}
