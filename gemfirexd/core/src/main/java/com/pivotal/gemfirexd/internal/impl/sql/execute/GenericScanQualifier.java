/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.GenericScanQualifier

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
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ScanQualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;


/**
 *	This is the implementation for ScanQualifier.  It is used for system and user
 *  scans.
 *
 *	@version 0.1
 */

public class GenericScanQualifier implements ScanQualifier
{

	private int                 columnId        = -1;
	//Gemstone changes BEGIN
	private String              columnName       = null;
	//Gemstone changes END
	private DataValueDescriptor orderable       = null;
	private int                 operator        = -1;
	private boolean             negateCR        = false;
	private boolean             orderedNulls    = false;
	private boolean             unknownRV       = false;

	private boolean             properInit      = false;

	public GenericScanQualifier() 
	{
	}

	/* 
	 * Qualifier interface
	 */

	/** 
	 * @see Qualifier#getColumnId
	 */
	public int getColumnId()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return columnId;
	}

	/** 
	 * @see Qualifier#getOrderable
	 */
	public DataValueDescriptor getOrderable()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return orderable;
	}

	/** Get the operator to use in the comparison. 
     *
     *  @see Qualifier#getOperator
     **/
	public int getOperator()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return operator;
	}

	/** Should the result from the compare operation be negated?  If true
     *  then only rows which fail the compare operation will qualify.
     *
     *  @see Qualifier#negateCompareResult
     **/
	public boolean negateCompareResult()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return negateCR;
	}

	/** Get the getOrderedNulls argument to use in the comparison. 
     *  
     *  @see Qualifier#getOrderedNulls
     **/
    public boolean getOrderedNulls()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return orderedNulls;
	}

	/** Get the getOrderedNulls argument to use in the comparison.
     *  
     *  @see Qualifier#getUnknownRV
     **/
    public boolean getUnknownRV()
	{
		if (SanityManager.DEBUG)
			SanityManager.ASSERT(properInit,	"properInit is expected to be true");
		return unknownRV;
	}

	/** Clear the DataValueDescriptor cache, if one exists.
	 *  (The DataValueDescriptor can be 1 of 3 types:
	 *		o  VARIANT		  - cannot be cached as its value can 
	 *							vary within a scan
	 *		o  SCAN_INVARIANT - can be cached within a scan as its
	 *							value will not change within a scan
	 *		o  QUERY_INVARIANT- can be cached across the life of the query
	 *							as its value will never change
	 *		o  CONSTANT		  - immutable
     *  
     *  @see Qualifier#getUnknownRV
	 */
	public void clearOrderableCache()
	{
		// No Orderable caching in ScanQualifiers
	}

	/** 
	 * This method reinitializes all the state of
	 * the Qualifier.  It is used to distinguish between
	 * resetting something that is query invariant
	 * and something that is constant over every
	 * execution of a query.  Basically, clearOrderableCache()
	 * will only clear out its cache if it is a VARIANT
	 * or SCAN_INVARIANT value.  However, each time a
	 * query is executed, the QUERY_INVARIANT qualifiers need
	 * to be reset.
	 */
	public void reinitialize()
	{
	}

	/*
	 * ScanQualifier interface
	 */

	/**
	 * @see ScanQualifier#setQualifier
	 */
	public void setQualifier(
    int                 columnId,
    //Gemstone changes BEGIN
    String              columnName,
    //Gemstone changes END
    DataValueDescriptor orderable, 
    int                 operator,
    boolean             negateCR, 
    boolean             orderedNulls, 
    boolean             unknownRV)
	{
		this.columnId = columnId;
		//Gemstone changes BEGIN
		this.columnName = columnName;
		//Gemstsone changes END
		this.orderable = orderable;
		this.operator = operator;
		this.negateCR = negateCR;
		this.orderedNulls = orderedNulls;
		this.unknownRV = unknownRV;
		properInit = true;
	}
// GemStone changes BEGIN

  @Override
  public void alignOrderableCache(com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor columnDescriptor, com.pivotal.gemfirexd.internal.engine.store.GemFireContainer container) {
  }

  @Override
  public String getColumnName() {
    return columnName;
  }
//GemStone changes END

}




