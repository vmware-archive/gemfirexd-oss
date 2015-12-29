/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GrantRoleConstantAction

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


import java.util.Iterator;
import java.util.List;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.RoleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 *  This class performs actions that are ALWAYS performed for a
 *  GRANT role statement at execution time.
 *
 *  Descriptors corresponding to the grants are stored in the
 *  SYS.SYSROLES table, along with the role definitions, cf
 *  CreateRoleConstantAction.
 *
 */
class GrantRoleConstantAction extends DDLConstantAction {

    private List roleNames;
    private List grantees;
    private final boolean withAdminOption = false; // not impl.

    // CONSTRUCTORS
    /**
     *  Make the ConstantAction for a CREATE ROLE statement.
     *  When executed, will create a role by the given name.
     *
     *  @param roleNames     List of the names of the roles being granted
     *  @param grantees       List of the authorization ids granted to role
     */
    public GrantRoleConstantAction(List roleNames, List grantees) {
        this.roleNames = roleNames;
        this.grantees = grantees;
    }

    // INTERFACE METHODS

    /**
     *  This is the guts of the Execution-time logic for GRANT role.
     *
     *  @see ConstantAction#executeConstantAction
     *
     * @exception StandardException     Thrown on failure
     */
    public void executeConstantAction(Activation activation)
            throws StandardException {

        LanguageConnectionContext lcc =
            activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        final String grantor = lcc.getAuthorizationId();

        dd.startWriting(lcc);

        for (Iterator rIter = roleNames.iterator(); rIter.hasNext();) {
            String role = (String)rIter.next();

            for (Iterator gIter = grantees.iterator(); gIter.hasNext();) {
                String grantee = (String)gIter.next();

                // check that role exists
                RoleDescriptor rd = dd.getRoleDefinitionDescriptor(role);

                if (rd == null) {
                    throw StandardException.
                        newException(SQLState.ROLE_INVALID_SPECIFICATION, role);
                }

                // Check that role is granted to us (or PUBLIC) with
                // WITH ADMIN option so we can grant it. For database
                // owner, a role definition always fulfills this
                // requirement.  If we implement granting with WITH ADMIN
                // option later, we need to look for a grant to us (or
                // PUBLIC) which has WITH ADMIN. The role definition
                // descriptor will not suffice in that case, so we
                // need something like:
                //
                // rd = dd.findRoleGrantWithAdminToRoleOrPublic(grantor)
                // if (rd != null) {
                //   :
                if (grantor.equals(rd.getGrantee())) {
                    // All ok, we are database owner
                    if (SanityManager.DEBUG) {
                        SanityManager.ASSERT(
                            lcc.getDataDictionary().
                            getAuthorizationDatabaseOwner().
                            equals(grantor),
                            "expected database owner in role descriptor");
                        SanityManager.ASSERT(
                            rd.isWithAdminOption(),
                            "expected role definition to have ADMIN OPTION");
                    }
                } else {
                    throw StandardException.newException
                        (SQLState.AUTH_ROLE_DBO_ONLY, "GRANT role");
                }

                rd = dd.getRoleGrantDescriptor(role, grantee, grantor);

                if (rd != null && withAdminOption && !rd.isWithAdminOption()) {
                    // NOTE: Never called yet, withAdminOption not yet
                    // implemented.

                    // Remove old descriptor and add a new one with admin
                    // option: cf. SQL 2003, section 12.5, general rule 3
                    rd.drop(lcc);
                    rd.setWithAdminOption(true);
                    dd.addDescriptor(rd,
                                     null,  // parent
                                     DataDictionary.SYSROLES_CATALOG_NUM,
                                     false, // no duplicatesAllowed
                                     tc);
                } else if (rd == null) {
                    RoleDescriptor gd = dd.getRoleDefinitionDescriptor(grantee);

                    if (gd != null) {
                        // FIXME: Grantee is role, need to check for circularity
                    }

                    rd = ddg.newRoleDescriptor(
                        dd.getUUIDFactory().createUUID(),
                        role,
                        grantee,
                        grantor, // dbo for now
                        withAdminOption,
                        false);  // not definition
                    dd.addDescriptor(
                        rd,
                        null,  // parent
                        DataDictionary.SYSROLES_CATALOG_NUM,
                        false, // no duplicatesAllowed
                        tc);
                } // else exists already, no need to add
            }
        }
    }


    // OBJECT SHADOWS

    public  String  toString()
    {
        // Do not put this under SanityManager.DEBUG - it is needed for
        // error reporting.

        StringBuilder sb1 = new StringBuilder();
        for (Iterator it = roleNames.iterator(); it.hasNext();) {
            if( sb1.length() > 0) {
                sb1.append( ", ");
            }
            sb1.append( it.next().toString());
        }

        StringBuilder sb2 = new StringBuilder();
        for (Iterator it = grantees.iterator(); it.hasNext();) {
            if( sb2.length() > 0) {
                sb2.append( ", ");
            }
            sb2.append( it.next().toString());
        }
        return ("GRANT " +
                sb1.toString() +
                " TO: " +
                sb2.toString() +
                "\n");
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
