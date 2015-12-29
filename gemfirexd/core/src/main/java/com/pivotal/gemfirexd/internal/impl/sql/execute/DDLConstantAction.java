/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DDLConstantAction

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;


import com.pivotal.gemfirexd.internal.catalog.AliasInfo;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.catalog.TypeDescriptor;
import com.pivotal.gemfirexd.internal.catalog.types.RoutineAliasInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.property.PropertyUtil;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.Authorizer;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.Dependent;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.AliasDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColPermsDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptorList;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DependencyDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.PermissionsDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementColumnPermission;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementPermission;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementRoutinePermission;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementSchemaPermission;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.StatementTablePermission;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.store.access.ConglomerateController;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;

/**
 * Abstract class that has actions that are across
 * all DDL actions.
 *
 */
// GemStone changes BEGIN
public abstract class DDLConstantAction implements ConstantAction
/* abstract class DDLConstantAction implements ConstantAction */
// GemStone changes END
{
	/**
	 * Get the schema descriptor for the schemaid.
	 *
	 * @param dd the data dictionary
	 * @param schemaId the schema id
	 * @param statementType string describing type of statement for error
	 *	reporting.  e.g. "ALTER STATEMENT"
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException if schema is system schema
	 */
	static SchemaDescriptor getAndCheckSchemaDescriptor(
						DataDictionary		dd,
						UUID				schemaId,
						String				statementType)
		throws StandardException
	{
		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaId, null);
		return sd;
	}

	/**
	 * Get the schema descriptor in the creation of an object in
	   the passed in schema.
	 *
	 * @param dd the data dictionary
	 * @param activation activation
	 * @param schemaName name of the schema
	 *
	 * @return the schema descriptor
	 *
	 * @exception StandardException if the schema does not exist
	 */
	/* GemStone change: static */ SchemaDescriptor getSchemaDescriptorForCreate(
						DataDictionary		dd,
						Activation activation,
						String schemaName)
		throws StandardException
	{
// GemStone changes BEGIN
	  this.implicitSchema = null;
// GemStone changes END
		TransactionController tc = activation.
			getLanguageConnectionContext().getTransactionExecute();

		SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, false);

		if (sd == null || sd.getUUID() == null) {
			CreateSchemaConstantAction csca
				= new CreateSchemaConstantAction(schemaName, (String) null);

			if (activation.getLanguageConnectionContext().
					isInitialDefaultSchema(schemaName)) {
				// DERBY-48: This operation creates the user's initial
				// default schema and we don't want to hold a lock for
				// SYSSCHEMAS for the duration of the user transaction
				// since connection attempts may block, so we perform
				// the creation in a nested transaction (if possible)
				// so we can commit at once and release locks.
				executeCAPreferSubTrans(csca, tc, activation);
			} else {
				// create the schema in the user transaction
				try {
					csca.executeConstantAction(activation);
// GemStone changes BEGIN
					this.implicitSchema = csca;
// GemStone changes END
				} catch (StandardException se) {
					if (se.getMessageId()
							.equals(SQLState.LANG_OBJECT_ALREADY_EXISTS)) {
						// Ignore "Schema already exists". Another thread has
						// probably created it after we checked for it
					} else {
						throw se;
					}
				}
			}


			sd = dd.getSchemaDescriptor(schemaName, tc, true);
		}

		return sd;
	}


	private static void executeCAPreferSubTrans
		(CreateSchemaConstantAction csca,
		 TransactionController tc,
		 Activation activation) throws StandardException {

		TransactionController useTc    = null;
		TransactionController nestedTc = null;

		try {
			nestedTc = tc.startNestedUserTransaction(false);
			useTc = nestedTc;
		} catch (StandardException e) {
			if (SanityManager.DEBUG) {
				SanityManager.THROWASSERT(
					"Unexpected: not able to start nested transaction " +
					"to auto-create schema", e);
			}
			useTc = tc;
		}

		// Try max twice: if nested transaction times out, try
		// again in the outer transaction because it may be a
		// self-lock, that is, the outer transaction may hold some
		// lock(s) that make the nested transaction attempt to set
		// a write lock time out.  Trying it again in the outer
		// transaction will then succeed. If the reason is some
		// other transaction barring us, trying again in the outer
		// transaction will possibly time out again.
		//
		// Also, if creating a nested transaction failed, only try
		// once in the outer transaction.
		while (true) {
			try {
				csca.executeConstantAction(activation, useTc);
			} catch (StandardException se) {
				if (se.getMessageId().equals(SQLState.LOCK_TIMEOUT)) {
					// We don't test for SQLState.DEADLOCK or
					// .LOCK_TIMEOUT_LOG here because a) if it is a
					// deadlock, it may be better to expose it, and b)
					// LOCK_TIMEOUT_LOG happens when the app has set
					// gemfirexd.locks.deadlockTrace=true, in which case we
					// don't want to mask the timeout.  So in both the
					// latter cases we just throw.
					if (useTc == nestedTc) {

						// clean up after use of nested transaction,
						// then try again in outer transaction
						useTc = tc;
						nestedTc.destroy();
						continue;
					}
				} else if (se.getMessageId()
							   .equals(SQLState.LANG_OBJECT_ALREADY_EXISTS)) {
					// Ignore "Schema already exists". Another thread has
					// probably created it after we checked for it
					break;
				}

				// We got an non-expected exception, either in
				// the nested transaction or in the outer
				// transaction; we had better pass that on
				if (useTc == nestedTc) {
					nestedTc.destroy();
				}

				throw se;
			}
			break;
		}

		// We either succeeded or got LANG_OBJECT_ALREADY_EXISTS.
		// Clean up if we did this in a nested transaction.
		if (useTc == nestedTc) {
			nestedTc.commit();
			nestedTc.destroy();
		}
	}


    /**
     * Adjust dependencies of a table on ANSI UDTs. We only add one dependency
     * between a table and a UDT. If the table already depends on the UDT, we don't add
     * a redundant dependency.
     */
    protected   void    adjustUDTDependencies
        (
         LanguageConnectionContext  lcc,
         DataDictionary             dd,
         TableDescriptor            td,
         ColumnInfo[]               columnInfos,
         boolean                    dropWholeTable
         )
        throws StandardException
    {
        if ( (!dropWholeTable) && (columnInfos == null) ) { return; }

		TransactionController tc = lcc.getTransactionExecute();

        int changedColumnCount = columnInfos == null ? 0 : columnInfos.length;
        HashMap addUdtMap = new HashMap();
        HashMap dropUdtMap = new HashMap();
        HashSet addColumnNames = new HashSet();
        HashSet dropColumnNames = new HashSet();

        // first find all of the new ansi udts which the table must depend on
        // and the old ones which are candidates for removal
        for ( int i = 0; i < changedColumnCount; i++ )
        {
            ColumnInfo ci = columnInfos[ i ];

            // skip this column if it is not a UDT
            AliasDescriptor ad = dd.getAliasDescriptorForUDT( tc, columnInfos[ i ].dataType );
            if ( ad == null ) { continue; }

            String key = ad.getObjectID().toString();

            if ( ci.action == ColumnInfo.CREATE )
            {
                addColumnNames.add( ci.name);

                // no need to add the descriptor if it is already on the list
                if ( addUdtMap.get( key ) != null ) { continue; }

                addUdtMap.put( key, ad );
            }
            else if ( ci.action == ColumnInfo.DROP )
            {
                dropColumnNames.add( ci.name );
                dropUdtMap.put( key, ad );
            }
        }

        // nothing to do if there are no changed columns of udt type
        // and this is not a DROP TABLE command
        if ( (!dropWholeTable) && (addUdtMap.size() == 0) && (dropUdtMap.size() == 0) ) { return; }

        //
        // Now prune from the add list all udt descriptors for which we already have dependencies.
        // These are the udts for old columns. This supports the ALTER TABLE ADD COLUMN
        // case.
        //
        // Also prune from the drop list add udt descriptors which will still be
        // referenced by the remaining columns.
        //
        ColumnDescriptorList cdl = td.getColumnDescriptorList();
        int totalColumnCount = cdl.size();

        for ( int i = 0; i < totalColumnCount; i++ )
        {
            ColumnDescriptor cd = cdl.elementAt( i );

            // skip columns that are being added and dropped. we only want the untouched columns
            if (
                addColumnNames.contains( cd.getColumnName() ) ||
                dropColumnNames.contains( cd.getColumnName() )
                ) { continue; }

            // nothing to do if the old column isn't a UDT
            AliasDescriptor ad = dd.getAliasDescriptorForUDT( tc, cd.getType() );
            if ( ad == null ) { continue; }

            String key = ad.getObjectID().toString();

            // ha, it is a UDT.
            if ( dropWholeTable ) { dropUdtMap.put( key, ad ); }
            else
            {
                if ( addUdtMap.get( key ) != null ) { addUdtMap.remove( key ); }
                if ( dropUdtMap.get( key ) != null ) { dropUdtMap.remove( key ); }
            }
        }

        adjustUDTDependencies( lcc, dd, td, addUdtMap, dropUdtMap );
    }
    /**
     * Add and drop dependencies of an object on UDTs.
     *
     * @param lcc Interpreter's state variable for this session.
     * @param dd Metadata
     * @param dependent Object which depends on UDT
     * @param addUdtMap Map of UDTs for which dependencies should be added
     * @param dropUdtMap Map of UDT for which dependencies should be dropped
     */
    private   void    adjustUDTDependencies
        (
         LanguageConnectionContext  lcc,
         DataDictionary             dd,
         Dependent                  dependent,
         HashMap                    addUdtMap,
         HashMap                    dropUdtMap
         )
        throws StandardException
    {
        // again, nothing to do if there are no columns of udt type
        if ( (addUdtMap.size() == 0) && (dropUdtMap.size() == 0) ) { return; }

		TransactionController tc = lcc.getTransactionExecute();
        DependencyManager     dm = dd.getDependencyManager();
        ContextManager        cm = lcc.getContextManager();

        // add new dependencies
        Iterator            addIterator = addUdtMap.values().iterator();
        while( addIterator.hasNext() )
        {
            AliasDescriptor ad = (AliasDescriptor) addIterator.next();

            dm.addDependency( dependent, ad, cm );
        }

        // drop dependencies that are orphaned
        Iterator            dropIterator = dropUdtMap.values().iterator();
        while( dropIterator.hasNext() )
        {
            AliasDescriptor ad = (AliasDescriptor) dropIterator.next();

            DependencyDescriptor dependency = new DependencyDescriptor( dependent, ad );

            dd.dropStoredDependency( dependency, tc );
        }
    }

    protected void invalidatePreparedStatement(Activation activation, int action) throws StandardException {
      LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
      ExecPreparedStatement st = activation.getPreparedStatement();
      if (st != null) {
        st.makeInvalid(action, lcc);  
      }
    }
    
    /**
     * Add and drop dependencies of a routine on UDTs.
     *
     * @param lcc Interpreter's state variable for this session.
     * @param dd Metadata
     * @param ad Alias descriptor for the routine
     * @param adding True if we are adding dependencies, false if we're dropping them
     */
    protected   void    adjustUDTDependencies
        (
         LanguageConnectionContext  lcc,
         DataDictionary             dd,
         AliasDescriptor            ad,
         boolean                    adding
         )
        throws StandardException
    {
        // nothing to do if this is not a routine
        switch ( ad.getAliasType() )
        {
		case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
		case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
            break;

        default: return;
        }
        
		TransactionController tc = lcc.getTransactionExecute();
        RoutineAliasInfo      aliasInfo = (RoutineAliasInfo) ad.getAliasInfo();
        HashMap               addUdtMap = new HashMap();
        HashMap               dropUdtMap = new HashMap();
        HashMap               udtMap = adding ? addUdtMap : dropUdtMap;
        TypeDescriptor        rawReturnType = aliasInfo.getReturnType();

        if ( rawReturnType != null )
        {
            AliasDescriptor       returnTypeAD = dd.getAliasDescriptorForUDT
                ( tc, DataTypeDescriptor.getType( rawReturnType ) );

            if ( returnTypeAD != null ) { udtMap.put( returnTypeAD.getObjectID().toString(), returnTypeAD ); }
        }

        // table functions can have udt columns. track those dependencies.
        if ( (rawReturnType != null) && rawReturnType.isRowMultiSet() )
        {
            TypeDescriptor[] columnTypes = rawReturnType.getRowTypes();
            int columnCount = columnTypes.length;

            for ( int i = 0; i < columnCount; i++ )
            {
                AliasDescriptor       columnTypeAD = dd.getAliasDescriptorForUDT
                    ( tc, DataTypeDescriptor.getType( columnTypes[ i ] ) );

                if ( columnTypeAD != null ) { udtMap.put( columnTypeAD.getObjectID().toString(), columnTypeAD ); }
            }
        }

        TypeDescriptor[]      paramTypes = aliasInfo.getParameterTypes();
        if ( paramTypes != null )
        {
            int paramCount = paramTypes.length;
            for ( int i = 0; i < paramCount; i++ )
            {
                AliasDescriptor       paramType = dd.getAliasDescriptorForUDT
                    ( tc, DataTypeDescriptor.getType( paramTypes[ i ] ) );

                if ( paramType != null ) { udtMap.put( paramType.getObjectID().toString(), paramType ); }
            }
        }

        adjustUDTDependencies( lcc, dd, ad, addUdtMap, dropUdtMap );
    }
    
	/**
	 * Lock the table in exclusive or share mode to prevent deadlocks.
	 *
	 * @param tc						The TransactionController
	 * @param heapConglomerateNumber	The conglomerate number for the heap.
	 * @param exclusiveMode				Whether or not to lock the table in exclusive mode.
	 *
	 * @exception StandardException if schema is system schema
	 */
	final void lockTableForDDL(TransactionController tc,
						 long heapConglomerateNumber, boolean exclusiveMode)
		throws StandardException
	{
		ConglomerateController cc;

		cc = tc.openConglomerate(
					heapConglomerateNumber,
                    false,
					(exclusiveMode) ?
						(TransactionController.OPENMODE_FORUPDATE | 
							TransactionController.OPENMODE_FOR_LOCK_ONLY) :
						TransactionController.OPENMODE_FOR_LOCK_ONLY,
			        TransactionController.MODE_TABLE,
                    TransactionController.ISOLATION_SERIALIZABLE);
		cc.close();
	}

	protected String constructToString(
						String				statementType,
						String              objectName)
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.

		return statementType + objectName;
	}
	
	
	/**
	 *	This method saves dependencies of constraints on privileges in the  
	 *  dependency system. It gets called by CreateConstraintConstantAction.
	 *  Views and triggers and constraints run with definer's privileges. If 
	 *  one of the required privileges is revoked from the definer, the 
	 *  dependent view/trigger/constraint on that privilege will be dropped 
	 *  automatically. In order to implement this behavior, we need to save 
	 *  view/trigger/constraint dependencies on required privileges in the 
	 *  dependency system. Following method accomplishes that part of the 
	 *  equation for constraints only. The dependency collection for 
	 *  constraints is not same as for views and triggers and hence 
	 *  constraints are handled by this special method.
	 * 	Views and triggers can depend on many different kind of privileges
	 *  where as constraints only depend on REFERENCES privilege on a table.
	 *  Another difference is only one view or trigger can be defined by a
	 *  sql statement and hence all the dependencies collected for the sql
	 *  statement apply to the view or trigger in question. As for constraints,
	 *  one sql statement can defined multiple constraints and hence the 
	 *  all the privileges required by the statement are not necessarily
	 *  required by all the constraints defined by that sql statement. We need
	 *  to identify right privileges for right constraints for a given sql
	 *  statement. Because of these differences between constraints and views
	 *  (and triggers), there are 2 different methods in this class to save
	 *  their privileges in the dependency system.
	 *   
	 *  @param activation The execution environment for this constant action.
	 *  @param dependent Make this object depend on required privileges
	 *  @param refTableUUID Make sure we are looking for REFERENCES privilege 
	 * 		for right table
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected void storeConstraintDependenciesOnPrivileges(
			Activation activation, Dependent dependent, UUID refTableUUID)
	throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		
		//If the Database Owner is creating this constraint, then no need to 
		//collect any privilege dependencies because the Database Owner can   
		//access any objects without any restrictions
		if (!(lcc.getAuthorizationId().equals(dd.getAuthorizationDatabaseOwner())))
		{
			PermissionsDescriptor permDesc;
			//Now, it is time to add into dependency system, constraint's 
			//dependency on REFERENCES privilege. If the REFERENCES privilege is 
			//revoked from the constraint owner, the constraint will get 
			//dropped automatically.
			List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
			if (requiredPermissionsList != null && ! requiredPermissionsList.isEmpty())
			{
				for(Iterator iter = requiredPermissionsList.iterator();iter.hasNext();)
				{
					StatementPermission statPerm = (StatementPermission) iter.next();
					//First check if we are dealing with a Table or 
					//Column level privilege. All the other privileges
					//are not required for a foreign key constraint.
					if (statPerm instanceof StatementTablePermission)
					{//It is a table/column level privilege
						StatementTablePermission statementTablePermission = 
							(StatementTablePermission) statPerm;
						//Check if we are dealing with REFERENCES privilege.
						//If not, move on to the next privilege in the
						//required privileges list
						if (statementTablePermission.getPrivType() != Authorizer.REFERENCES_PRIV)
							continue;
						//Next check is this REFERENCES privilege is 
						//on the same table as referenced by the foreign
						//key constraint? If not, move on to the next
						//privilege in the required privileges list
						if (!statementTablePermission.getTableUUID().equals(refTableUUID))
							continue;
					} else if (statPerm instanceof StatementSchemaPermission 
							|| statPerm instanceof StatementRoutinePermission)
						continue;

					//We know that we are working with a REFERENCES 
					//privilege. Find all the PermissionDescriptors for
					//this privilege and make constraint depend on it
					//through dependency manager.
					//The REFERENCES privilege could be defined at the
					//table level or it could be defined at individual
					//column levels. In addition, individual column
					//REFERENCES privilege could be available at the
					//user level or PUBLIC level.
					permDesc = statPerm.getPermissionDescriptor(lcc.getAuthorizationId(), dd);				
					if (permDesc == null) 
					{
						//No REFERENCES privilege exists for given 
						//authorizer at table or column level.
						//REFERENCES privilege has to exist at at PUBLIC level
						permDesc = statPerm.getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd);
						if (!(permDesc.checkOwner(lcc.getAuthorizationId())))
							dm.addDependency(dependent, permDesc, lcc.getContextManager());
					} else 
						//if the object on which permission is required is owned by the
						//same user as the current user, then no need to keep that
						//object's privilege dependency in the dependency system
					if (!(permDesc.checkOwner(lcc.getAuthorizationId())))
					{
						dm.addDependency(dependent, permDesc, lcc.getContextManager());
						if (permDesc instanceof ColPermsDescriptor)
						{
							//The if statement above means we found a
							//REFERENCES privilege at column level for
							//the given authorizer. If this privilege
							//doesn't cover all the column , then there 
							//has to exisit REFERENCES for the remaining
							//columns at PUBLIC level. Get that permission
							//descriptor and save it in dependency system
							StatementColumnPermission statementColumnPermission = (StatementColumnPermission) statPerm;
							permDesc = statementColumnPermission.getPUBLIClevelColPermsDescriptor(lcc.getAuthorizationId(), dd);
							//Following if checks if some column level privileges
							//exist only at public level. If so, then the public
							//level column privilege dependency is added
							//into the dependency system
							if (permDesc != null)
								dm.addDependency(dependent, permDesc, lcc.getContextManager());	           																
						}
					}
					//We have found the REFERENCES privilege for all the
					//columns in foreign key constraint and we don't 
					//need to go through the rest of the privileges
					//for this sql statement.
					break;																										
				}
			}
		}
		
	}	
	
	/**
	 *	This method saves dependencies of views and triggers on privileges in  
	 *  the dependency system. It gets called by CreateViewConstantAction
	 *  and CreateTriggerConstantAction. Views and triggers and constraints
	 *  run with definer's privileges. If one of the required privileges is
	 *  revoked from the definer, the dependent view/trigger/constraint on
	 *  that privilege will be dropped automatically. In order to implement 
	 *  this behavior, we need to save view/trigger/constraint dependencies 
	 *  on required privileges in the dependency system. Following method 
	 *  accomplishes that part of the equation for views and triggers. The
	 *  dependency collection for constraints is not same as for views and
	 *  triggers and hence constraints are not covered by this method.
	 *  Views and triggers can depend on many different kind of privileges
	 *  where as constraints only depend on REFERENCES privilege on a table.
	 *  Another difference is only one view or trigger can be defined by a
	 *  sql statement and hence all the dependencies collected for the sql
	 *  statement apply to the view or trigger in question. As for constraints,
	 *  one sql statement can defined multiple constraints and hence the 
	 *  all the privileges required by the statement are not necessarily
	 *  required by all the constraints defined by that sql statement. We need
	 *  to identify right privileges for right constraints for a given sql
	 *  statement. Because of these differences between constraints and views
	 *  (and triggers), there are 2 different methods in this class to save
	 *  their privileges in the dependency system.  
	 *
	 *  @param activation The execution environment for this constant action.
	 *  @param dependent Make this object depend on required privileges
	 *
	 * @exception StandardException		Thrown on failure
	 */
	protected void storeViewTriggerDependenciesOnPrivileges(
			Activation activation, Dependent dependent)
	throws StandardException
	{
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
		
		//If the Database Owner is creating this view/triiger, then no need to  
		//collect any privilege dependencies because the Database Owner can  
		//access any objects without any restrictions
		if (!(lcc.getAuthorizationId().equals(dd.getAuthorizationDatabaseOwner())))
		{
			PermissionsDescriptor permDesc;
			List requiredPermissionsList = activation.getPreparedStatement().getRequiredPermissionsList();
			if (requiredPermissionsList != null && ! requiredPermissionsList.isEmpty())
			{
				for(Iterator iter = requiredPermissionsList.iterator();iter.hasNext();)
				{
					StatementPermission statPerm = (StatementPermission) iter.next();
					//The schema ownership permission just needs to be checked 
					//at object creation time, to see if the object creator has 
					//permissions to create the object in the specified schema. 
					//But we don't need to add schema permission to list of 
					//permissions that the object is dependent on once it is 
					//created.
					if (statPerm instanceof StatementSchemaPermission)
						continue;
					//See if we can find the required privilege for given authorizer?
					permDesc = statPerm.getPermissionDescriptor(lcc.getAuthorizationId(), dd);				
					if (permDesc == null)//privilege not found for given authorizer 
					{
						//The if condition above means that required privilege does 
						//not exist at the user level. The privilege has to exist at 
						//PUBLIC level.
						permDesc = statPerm.getPermissionDescriptor(Authorizer.PUBLIC_AUTHORIZATION_ID, dd);
						//If the user accessing the object is the owner of that 
						//object, then no privilege tracking is needed for the
						//owner.
						if (!(permDesc.checkOwner(lcc.getAuthorizationId())))
							dm.addDependency(dependent, permDesc, lcc.getContextManager());
						continue;
					}
					//if the object on which permission is required is owned by the
					//same user as the current user, then no need to keep that
					//object's privilege dependency in the dependency system
					if (!(permDesc.checkOwner(lcc.getAuthorizationId())))
					{
						dm.addDependency(dependent, permDesc, lcc.getContextManager());	           							
						if (permDesc instanceof ColPermsDescriptor)
						{
							//For a given table, the table owner can give privileges
							//on some columns at individual user level and privileges
							//on some columns at PUBLIC level. Hence, when looking for
							//column level privileges, we need to look both at user
							//level as well as PUBLIC level(only if user level column
							//privileges do not cover all the columns accessed by this
							//object). We have finished adding dependency for user level 
							//columns, now we are checking if some required column 
							//level privileges are at PUBLIC level.
							//A specific eg of a view
							//user1
							//create table t11(c11 int, c12 int);
							//grant select(c11) on t1 to user2;
							//grant select(c12) on t1 to PUBLIC;
							//user2
							//create view v1 as select c11 from user1.t11 where c12=2;
							//For the view above, there are 2 column level privilege 
							//depencies, one for column c11 which exists directly
							//for user2 and one for column c12 which exists at PUBLIC level.
							StatementColumnPermission statementColumnPermission = (StatementColumnPermission) statPerm;
							permDesc = statementColumnPermission.getPUBLIClevelColPermsDescriptor(lcc.getAuthorizationId(), dd);
							//Following if checks if some column level privileges
							//exist only at public level. If so, then the public
							//level column privilege dependency of view is added
							//into dependency system.
							if (permDesc != null)
								dm.addDependency(dependent, permDesc, lcc.getContextManager());	           							
						}
					}
				}
			}
			
		}
	}
// GemStone changes BEGIN

  // rename index and/or changes its conglomID at execute time
  void updateIndex(SchemaDescriptor sd, TableDescriptor td,
      Activation activation, String oldName, String newName, long newConglomId)
      throws StandardException {
    LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
    DataDictionary dd = lcc.getDataDictionary();
    DependencyManager dm = dd.getDependencyManager();
    TransactionController tc = lcc.getTransactionExecute();
    // for indexes, we only invalidate sps, rest we ignore(ie views)
    dm.invalidateFor(td, DependencyManager.RENAME_INDEX, lcc);

    ConglomerateDescriptor conglomerateDescriptor = dd
        .getConglomerateDescriptor(oldName, sd, true);

    if (conglomerateDescriptor == null)
      throw StandardException.newException(
          SQLState.LANG_INDEX_NOT_FOUND_DURING_EXECUTION, oldName);

    /* Drop the index descriptor */
    dd.dropConglomerateDescriptor(conglomerateDescriptor, tc);
    // Change the index name of the index descriptor
    conglomerateDescriptor.setConglomerateName(newName);
    // Change the conglomerate number if required
    if (newConglomId >= 0) {
      conglomerateDescriptor.setConglomerateNumber(newConglomId);
    }
    // add the index descriptor with new name
    dd.addDescriptor(conglomerateDescriptor, sd,
        DataDictionary.SYSCONGLOMERATES_CATALOG_NUM, false, tc);
  }

  // methods for DDL conflation

  /**
   * return ConstantAction of any implicit schema was created for this CREATE
   * TABLE
   */
  private CreateSchemaConstantAction implicitSchema;

  /**
   * true if this is a user created object that can be distributed to others and
   * replayed by new servers
   */
  public boolean isReplayable() {
    return true;
  }

  /**
   * Get the schema name (if any) for the DDL. Conflation mechanism will only
   * come into play if this is non-null.
   */
  public abstract String getSchemaName();

  /** Get the table name (if any) for the DDL. */
  public String getTableName() {
    return null;
  }

  /**
   * Return true if this is a drop statement which will invoke conflation for
   * this DDL.
   */
  public boolean isDropStatement() {
    return false;
  }

  public boolean isDropIfExists() {
    return false;
  }

  /**
   * Get the name of object (for non table/schema level DDLs) used for
   * determining conflatable entries
   */
  public String getObjectName() {
    return null;
  }

  @Override
  public boolean isCancellable() {
    return true;
  }

  /**
   * Returns any implicit schema created in the execution of CREATE
   * TABLE/VIEW/... else null.
   */
  public CreateSchemaConstantAction getImplicitSchemaCreated() {
    return this.implicitSchema;
  }
// GemStone changes END
}

