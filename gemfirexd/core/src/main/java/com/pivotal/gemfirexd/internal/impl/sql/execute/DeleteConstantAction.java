/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.execute.DeleteConstantAction

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
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.ArrayUtil;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatIdUtil;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultDescription;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.IndexRowGenerator;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import java.util.Properties;


/**
 *	This class  describes compiled constants that are passed into
 *	DeleteResultSets.
 *
 */

public class DeleteConstantAction extends WriteCursorConstantAction
{
	/********************************************************
	**
	**	This class implements Formatable. But it is NOT used
 	**	across either major or minor releases.  It is only
	** 	written persistently in stored prepared statements, 
	**	not in the replication stage.  SO, IT IS OK TO CHANGE
	**	ITS read/writeExternal.
	**
	********************************************************/
	
	int numColumns;
	ConstantAction[] dependentCActions; //constant action for the dependent table
	ResultDescription resultDescription; //required for dependent tables.

	// CONSTRUCTORS

	/**
	 * Public niladic constructor. Needed for Formatable interface to work.
	 *
	 */
    public	DeleteConstantAction() { super(); }

	/**
	 *	Make the ConstantAction for an DELETE statement.
	 *
	 *  @param conglomId	Conglomerate ID.
	 *	@param heapSCOCI	StaticCompiledOpenConglomInfo for heap.
	 *  @param irgs			Index descriptors
	 *  @param indexCIDS	Conglomerate IDs of indices
	 *	@param indexSCOCIs	StaticCompiledOpenConglomInfos for indexes.
	 *  @param emptyHeapRow	Template for heap row.
	 *  @param deferred		True means process as a deferred insert.
	 *	@param targetUUID	UUID of target table
	 *	@param lockMode		The lock mode to use
	 *							(row or table, see TransactionController)
	 *	@param fkInfo		Array of structures containing foreign key info, if any (may be null)
	 *	@param triggerInfo	Array of structures containing trigger info, if any (may be null)
	 *  @param baseRowReadList Map of columns read in.  1 based.
     *  @param streamStorableHeapColIds Null for non rep. (0 based)
	 *  @param numColumns	Number of columns to read.
	 *  @param singleRowSource		Whether or not source is a single row source
	 */
	public	DeleteConstantAction(
								long				conglomId,
								StaticCompiledOpenConglomInfo heapSCOCI,
								IndexRowGenerator[]	irgs,
								long[]				indexCIDS,
								StaticCompiledOpenConglomInfo[] indexSCOCIs,
								ExecRow				emptyHeapRow,
								boolean				deferred,
								UUID				targetUUID,
								int					lockMode,
								FKInfo[]			fkInfo,
								TriggerInfo			triggerInfo,
								FormatableBitSet				baseRowReadList,
								int[]				baseRowReadMap,
								int[]               streamStorableHeapColIds,
								int					numColumns,
								boolean				singleRowSource,
								ResultDescription   resultDescription,
								ConstantAction[] dependentCActions)
	{
		super( conglomId, 
			   heapSCOCI,
			   irgs, indexCIDS, indexSCOCIs, 
			   null, // index names not needed for delete.
			   deferred, 
			   (Properties) null,
			   targetUUID,
			   lockMode,
			   fkInfo,
			   triggerInfo,
			   emptyHeapRow,
			   baseRowReadList,
			   baseRowReadMap,
			   streamStorableHeapColIds,
			   singleRowSource
			   );

		this.numColumns = numColumns;
		this.resultDescription = resultDescription;
		this.dependentCActions =  dependentCActions;
	}

	// INTERFACE METHODS

	// Formatable methods

	/**
	  @see java.io.Externalizable#readExternal
	  @exception IOException thrown on error
	  @exception ClassNotFoundException	thrown on error
	  */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		numColumns = in.readInt();

		//information required for cascade delete
		dependentCActions = new ConstantAction[ArrayUtil.readArrayLength(in)];
		ArrayUtil.readArrayItems(in, dependentCActions);
		resultDescription = (ResultDescription) in.readObject();

	}

	/**

	  @exception IOException thrown on error
	  */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		super.writeExternal(out);
		out.writeInt(numColumns);

		//write cascade delete information
		ArrayUtil.writeArray(out, dependentCActions);
		out.writeObject(resultDescription);

	}

	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.DELETE_CONSTANT_ACTION_V01_ID; }

// GemStone changes BEGIN    
  @Override
  public boolean isCancellable() {
    return false;
  }
// GemStone changes END    
	// KeyToBaseRowConstantAction METHODS
}
