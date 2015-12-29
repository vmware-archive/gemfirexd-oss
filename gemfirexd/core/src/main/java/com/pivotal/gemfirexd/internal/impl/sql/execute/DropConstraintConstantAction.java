/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DropConstraintConstantAction

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
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableHashtable;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.StatementType;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConstraintDescriptorList;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ForeignKeyConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ReferencedKeyConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;

/**
 *	This class  describes actions that are ALWAYS performed for a
 *	drop constraint at Execution time.
 *
 *	@version 0.1
 */

public class DropConstraintConstantAction extends ConstraintConstantAction
{

	private boolean cascade;		// default false
	private String constraintSchemaName;
    private int verifyType;
    private int constraintIdentifier;

	// CONSTRUCTORS

	/**
	 *	Make one of these puppies.
	 *
	 *  @param constraintName	Constraint name.
	 *  @param constraintSchemaName		the schema that constraint lives in.
	 *  @param tableName		Table name.
	 *	@param tableId			UUID of table.
	 *  @param tableSchemaName				the schema that table lives in.
	 *  @param indexAction		IndexConstantAction for constraint (if necessary)
	 *  @param behavior			the drop behavior (e.g. StatementType.DROP_CASCADE)
	 */
	DropConstraintConstantAction(
		               String				constraintName,
					   String				constraintSchemaName,
		               String				tableName,
					   UUID					tableId,
					   String				tableSchemaName,
					   IndexConstantAction indexAction,
					   int					behavior,
                       int                  verifyType)
	{
		super(constraintName, DataDictionary.DROP_CONSTRAINT, tableName, 
			  tableId, tableSchemaName, indexAction);

		cascade = (behavior == StatementType.DROP_CASCADE);
		this.constraintSchemaName = constraintSchemaName;
        this.verifyType = verifyType;
	}

	// OBJECT METHODS

	public	String	toString()
	{
		// Do not put this under SanityManager.DEBUG - it is needed for
		// error reporting.
		if (constraintName == null)
			return "DROP PRIMARY KEY";

		String ss = constraintSchemaName == null ? schemaName : constraintSchemaName;
		return "DROP CONSTRAINT " + ss + "." + constraintName;
	}

	// INTERFACE METHODS


	/**
	 *	This is the guts of the Execution-time logic for DROP CONSTRAINT.
	 *
	 *	@see ConstantAction#executeConstantAction
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public void	executeConstantAction( Activation activation )
						throws StandardException
	{
		ConstraintDescriptor		conDesc = null;
		TableDescriptor				td;
		UUID							indexId = null;
		String						indexUUIDString;

		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
		DataDictionary dd = lcc.getDataDictionary();
		DependencyManager dm = dd.getDependencyManager();
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

		td = dd.getTableDescriptor(tableId);

		if (td == null)
		{
			throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND_DURING_EXECUTION, tableName);
		}

		/* Table gets locked in AlterTableConstantAction */

		/*
		** If the schema descriptor is null, then
		** we must have just read ourselves in.  
		** So we will get the corresponding schema
		** descriptor from the data dictionary.
		*/

		SchemaDescriptor tdSd = td.getSchemaDescriptor();
		SchemaDescriptor constraintSd = 
			constraintSchemaName == null ? tdSd : dd.getSchemaDescriptor(constraintSchemaName, tc, true);


		/* Get the constraint descriptor for the index, along
		 * with an exclusive row lock on the row in sys.sysconstraints
		 * in order to ensure that no one else compiles against the
		 * index.
		 */
		if (constraintName == null)  // this means "alter table drop primary key"
			conDesc = dd.getConstraintDescriptors(td).getPrimaryKey();
		else
			conDesc = dd.getConstraintDescriptorByName(td, constraintSd, constraintName, true);

		// Error if constraint doesn't exist
		if (conDesc == null)
		{
			String errorName = constraintName == null ? "PRIMARY KEY" :
								(constraintSd.getSchemaName() + "."+ constraintName);

			throw StandardException.newException(SQLState.LANG_DROP_NON_EXISTENT_CONSTRAINT, 
						errorName,
						td.getQualifiedName());
		}
// GemStone changes BEGIN
    final int constraintType = constraintIdentifier = conDesc.getConstraintType();
    // GemFireXD cannot handle PK drop with data or buckets (#40712)
    if (constraintType == DataDictionary.PRIMARYKEY_CONSTRAINT) {
      int numBuckets = AlterTableConstantAction.getNumBucketsOrSize(Misc
          .getRegionForTableByPath(Misc.getFullTableName(td, lcc), true), lcc);
      if (numBuckets > 0) {
        throw StandardException.newException(SQLState.NOT_IMPLEMENTED,
            "PRIMARY KEY drop in ALTER TABLE with data or data history");
      }
    }

// GemStone changes END
        switch( verifyType)
        {
        case DataDictionary.UNIQUE_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "UNIQUE");
            break;

        case DataDictionary.CHECK_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "CHECK");
            break;

        case DataDictionary.FOREIGNKEY_CONSTRAINT:
            if( conDesc.getConstraintType() != verifyType)
                throw StandardException.newException(SQLState.LANG_DROP_CONSTRAINT_TYPE,
                                                     constraintName, "FOREIGN KEY");
            break;
        }

		boolean cascadeOnRefKey = (cascade && 
						conDesc instanceof ReferencedKeyConstraintDescriptor);
		if (!cascadeOnRefKey)
		{
			dm.invalidateFor(conDesc, DependencyManager.DROP_CONSTRAINT, lcc);
		}

// GemStone changes BEGIN
		// refresh parent table
		if (constraintType == DataDictionary.FOREIGNKEY_CONSTRAINT) {
		  dm.invalidateFor(((ForeignKeyConstraintDescriptor)conDesc).
		      getReferencedConstraint().getTableDescriptor(),
		      DependencyManager.DROP_CONSTRAINT, lcc);
		}
// GemStone changes END

		/*
		** If we had a primary/unique key and it is drop cascade,	
		** drop all the referencing keys now.  We MUST do this AFTER
		** dropping the referenced key because otherwise we would
		** be repeatedly changing the reference count of the referenced
		** key and generating unnecessary I/O.
		*/
		dropConstraint(conDesc, activation, lcc, !cascadeOnRefKey);

		if (cascadeOnRefKey) 
		{
			ForeignKeyConstraintDescriptor fkcd;
			ReferencedKeyConstraintDescriptor cd;
			ConstraintDescriptorList cdl;

			cd = (ReferencedKeyConstraintDescriptor)conDesc;
			cdl = cd.getForeignKeyConstraints(ReferencedKeyConstraintDescriptor.ALL);
			int cdlSize = cdl.size();

			for(int index = 0; index < cdlSize; index++)
			{
				fkcd = (ForeignKeyConstraintDescriptor) cdl.elementAt(index);
				dm.invalidateFor(fkcd, DependencyManager.DROP_CONSTRAINT, lcc);
				dropConstraint(fkcd, activation, lcc, true);
			}
	
			/*
			** We told dropConstraintAndIndex not to
			** remove our dependencies, so send an invalidate,
			** and drop the dependencies.
			*/
			dm.invalidateFor(conDesc, DependencyManager.DROP_CONSTRAINT, lcc);
			dm.clearDependencies(lcc, conDesc);
		}
	}
// GemStone changes BEGIN

  @Override
  public String getSchemaName() {
    return null;
  }

  @Override
  String getFullConstraintName() {
    return this.constraintSchemaName + '.' + this.constraintName;
  }
  
  public boolean isForeignKeyConstraint() {
    return this.constraintIdentifier == DataDictionary.FOREIGNKEY_CONSTRAINT;
  }
  
// GemStone changes END
}
