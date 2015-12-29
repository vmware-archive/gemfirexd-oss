/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.SYSCHECKSRowFactory

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

package com.pivotal.gemfirexd.internal.impl.sql.catalog;

import java.sql.Types;

import com.pivotal.gemfirexd.internal.catalog.ReferencedColumns;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.CatalogRowFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.CheckConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SubCheckConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;

/**
 * Factory for creating a SYSCHECKS row.
 *
 */

class SYSCHECKSRowFactory extends CatalogRowFactory
{
	private  static final String	TABLENAME_STRING = "SYSCHECKS";

	private static final int		SYSCHECKS_COLUMN_COUNT = 3;
	private static final int		SYSCHECKS_CONSTRAINTID = 1;
	private static final int		SYSCHECKS_CHECKDEFINITION = 2;
	private static final int		SYSCHECKS_REFERENCEDCOLUMNS = 3;

	static final int		SYSCHECKS_INDEX1_ID = 0;

	// index is unique.
    private	static	final	boolean[]	uniqueness = null;

	private static final int[][] indexColumnPositions =
	{	
		{SYSCHECKS_CONSTRAINTID}
	};

	private	static	final	String[]	uuids =
	{
		 "80000056-00d0-fd77-3ed8-000a0a0b1900"	// catalog UUID
		,"80000059-00d0-fd77-3ed8-000a0a0b1900"	// heap UUID
		,"80000058-00d0-fd77-3ed8-000a0a0b1900"	// SYSCHECKS_INDEX1 UUID
	};



	/////////////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	/////////////////////////////////////////////////////////////////////////////

	SYSCHECKSRowFactory(UUIDFactory uuidf, ExecutionFactory ef, DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		initInfo(SYSCHECKS_COLUMN_COUNT, TABLENAME_STRING, indexColumnPositions, uniqueness, uuids );
	}

	/////////////////////////////////////////////////////////////////////////////
	//
	//	METHODS
	//
	/////////////////////////////////////////////////////////////////////////////

  /**
	 * Make a SYSCHECKS row
	 *
	 * @param td CheckConstraintDescriptorImpl
	 *
	 * @return	Row suitable for inserting into SYSCHECKS.
	 *
	 * @exception   StandardException thrown on failure
	 */
	public ExecRow makeRow(TupleDescriptor	td, TupleDescriptor parent)
					throws StandardException 
	{
		DataValueDescriptor		col;
		ExecIndexRow			row;
		ReferencedColumns rcd = null;
		String					checkDefinition = null;
		String					constraintID = null;

		if (td != null)
		{
			CheckConstraintDescriptor cd = (CheckConstraintDescriptor)td;
			/*
			** We only allocate a new UUID if the descriptor doesn't already have one.
			** For descriptors replicated from a Source system, we already have an UUID.
			*/
			constraintID = cd.getUUID().toString();

			checkDefinition = cd.getConstraintText();

			rcd = cd.getReferencedColumnsDescriptor();
		}

		/* Build the row */
		row = getExecutionFactory().getIndexableRow(SYSCHECKS_COLUMN_COUNT);

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		row.setColumn(SYSCHECKS_CONSTRAINTID, new SQLChar(constraintID));

		/* 2nd column is CHECKDEFINITION */
		row.setColumn(SYSCHECKS_CHECKDEFINITION,
				dvf.getLongvarcharDataValue(checkDefinition));

		/* 3rd column is REFERENCEDCOLUMNS
		 *  (user type com.pivotal.gemfirexd.internal.catalog.ReferencedColumns)
		 */
		row.setColumn(SYSCHECKS_REFERENCEDCOLUMNS,
			dvf.getDataValue(rcd));

		return row;
	}

	///////////////////////////////////////////////////////////////////////////
	//
	//	ABSTRACT METHODS TO BE IMPLEMENTED BY CHILDREN OF CatalogRowFactory
	//
	///////////////////////////////////////////////////////////////////////////

	/**
	 * Make a ViewDescriptor out of a SYSCHECKS row
	 *
	 * @param row a SYSCHECKS row
	 * @param parentTupleDescriptor	Null for this kind of descriptor.
	 * @param dd dataDictionary
	 *
	 * @exception   StandardException thrown on failure
	 */
	public TupleDescriptor buildDescriptor(
		ExecRow					row,
		TupleDescriptor			parentTupleDescriptor,
		DataDictionary 			dd )
					throws StandardException
	{
		SubCheckConstraintDescriptor checkDesc = null;

		if (SanityManager.DEBUG)
		{
			SanityManager.ASSERT(
				row.nColumns() == SYSCHECKS_COLUMN_COUNT, 
				"Wrong number of columns for a SYSCHECKS row");
		}

		DataValueDescriptor		col;
		DataDescriptorGenerator ddg;
		ReferencedColumns	referencedColumns;
		String				constraintText;
		String				constraintUUIDString;
		UUID				constraintUUID;

		ddg = dd.getDataDescriptorGenerator();

		/* 1st column is CONSTRAINTID (UUID - char(36)) */
		col = row.getColumn(SYSCHECKS_CONSTRAINTID);
		constraintUUIDString = col.getString();
		constraintUUID = getUUIDFactory().recreateUUID(constraintUUIDString);

		/* 2nd column is CHECKDEFINITION */
		col = row.getColumn(SYSCHECKS_CHECKDEFINITION);
		constraintText = col.getString();

		/* 3rd column is REFERENCEDCOLUMNS */
		col = row.getColumn(SYSCHECKS_REFERENCEDCOLUMNS);
		referencedColumns =
			(ReferencedColumns) col.getObject();

		/* now build and return the descriptor */

		checkDesc = new SubCheckConstraintDescriptor(
										constraintUUID,
										constraintText,
										referencedColumns);
		return checkDesc;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */

    public SystemColumn[] buildColumnList()
        throws StandardException
    {
        
       return new SystemColumn[] {
            SystemColumnImpl.getUUIDColumn("CONSTRAINTID", false),
            SystemColumnImpl.getColumn("CHECKDEFINITION", Types.LONGVARCHAR, false),
            SystemColumnImpl.getJavaColumn("REFERENCEDCOLUMNS",
                    "com.pivotal.gemfirexd.internal.catalog.ReferencedColumns", false)             
        };
    }
}
