/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.catalog.SYSDUMMY1RowFactory

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



import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.services.uuid.UUIDFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.CatalogRowFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDescriptorGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TupleDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecutionFactory;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;

/**
 * Factory for creating a SYSDUMMY1 row.
 *
 * @version 0.01
 *
 */

class SYSDUMMY1RowFactory extends CatalogRowFactory
{
	protected static final int SYSDUMMY1_COLUMN_COUNT = 1;

	private static final String[] uuids =
	{
		"c013800d-00f8-5b70-bea3-00000019ed88", // catalog UUID
		"c013800d-00f8-5b70-fee8-000000198c88"  // heap UUID.
	};

	/*
	 *	CONSTRUCTORS
	 */
    SYSDUMMY1RowFactory(UUIDFactory uuidf, 
									ExecutionFactory ef, 
									DataValueFactory dvf)
	{
		super(uuidf,ef,dvf);
		
		initInfo(SYSDUMMY1_COLUMN_COUNT, "SYSDUMMY1", 
				 null, null, uuids);
	}


  /**
	 * Make a SYSDUMMY1 row
	 *
	 *
	 * @return	Row suitable for inserting into SYSSTATISTICS.
	 *
	 * @exception   StandardException thrown on failure
	 */

	public ExecRow makeRow(TupleDescriptor td, TupleDescriptor parent)
					throws StandardException					
	{
		ExecRow row = getExecutionFactory().getValueRow(SYSDUMMY1_COLUMN_COUNT);
		
		row.setColumn(1, new SQLChar("Y"));
		return row;
	}
	
	public TupleDescriptor buildDescriptor(
		 ExecRow 			row,
		 TupleDescriptor    parentDesc,
		 DataDictionary 	dd)
		throws StandardException
		 
	{
		return null;
	}

	/**
	 * Builds a list of columns suitable for creating this Catalog.
	 *
	 *
	 * @return array of SystemColumn suitable for making this catalog.
	 */
	public SystemColumn[] buildColumnList()
	{        
        return new SystemColumn[] {
                SystemColumnImpl.getColumn("IBMREQD", Types.CHAR, true, 1)
        };
	}


}	
