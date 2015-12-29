/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.IndexConstantAction

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
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

/**
 *	This class is the superclass for the classes that describe actions 
 *  that are ALWAYS performed for a CREATE/DROP INDEX Statement at Execution time.
 *
 */

public abstract class IndexConstantAction extends DDLSingleTableConstantAction
{

	String				indexName;
	String				tableName;
	String				schemaName;

	// CONSTRUCTORS

	/**
	 *	Make the ConstantAction for a CREATE/DROP INDEX statement.
	 *
	 *	@param	tableId				The table uuid
	 *	@param	indexName			Index name.
	 *	@param	tableName			The table name
	 *	@param	schemaName					Schema that index lives in.
	 *
	 */
	protected	IndexConstantAction(
								UUID				tableId,
								String				indexName,
								String				tableName,
								String schemaName)
	{
		super(tableId);
		this.indexName = indexName;
		this.tableName = tableName;
		this.schemaName = schemaName;

// GemStone changes BEGIN
// [sjigyasu] Moved the following code from this base class to derived class CreatedIndexConstantAction since we want to allow
// null schema when we check for IF EXISTS in DropIndexConstantAction.		
//		if (SanityManager.DEBUG)
//		{
//			SanityManager.ASSERT(schemaName != null, "Schema name is null");
//		}
// GemStone changes END		
	}

    // CLASS METHODS

	/**
	  *	Get the index name.
	  *
	  *	@return	the name of the index
	  */
    public	String	getIndexName() { return indexName; }

	/**
	 * Set the index name at execution time.
	 * Useful for unnamed constraints which have a backing index.
	 *
	 * @param indexName		The (generated) index name.
	 */
	public void setIndexName(String indexName)
	{
		this.indexName = indexName;
	}
}
