/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.DropTableNode

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

package	com.pivotal.gemfirexd.internal.impl.sql.compile;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * A DropTableNode is the root of a QueryTree that represents a DROP TABLE
 * statement.
 *
 */

public class DropTableNode extends DDLStatementNode
{
	private long		conglomerateNumber;
// GemStone changes BEGIN
	private boolean onlyIfExists;
	private SchemaDescriptor sd;
// GemStone changes END
	private int			dropBehavior;
	private	TableDescriptor	td;

	/**
	 * Intializer for a DropTableNode
	 *
	 * @param dropObjectName	The name of the object being dropped
	 * @param dropBehavior		Drop behavior (RESTRICT | CASCADE)
	 *
	 */

// GemStone changes BEGIN
	public void init(Object dropObjectName, Object onlyIfExists,
	    Object dropBehavior)
	/* (original code)
	public void init(Object dropObjectName, Object dropBehavior)
	*/
// GemStone changes END
		throws StandardException
	{
		initAndCheck(dropObjectName);
		this.dropBehavior = ((Integer) dropBehavior).intValue();
// GemStone changes BEGIN
		this.onlyIfExists = ((Boolean)onlyIfExists).booleanValue();
// GemStone changes END
	}

	/**
	 * Convert this object to a String.  See comments in QueryTreeNode.java
	 * for how this should be done for tree printing.
	 *
	 * @return	This object as a String
	 */

	public String toString()
	{
		if (SanityManager.DEBUG)
		{
			return super.toString() +
				"conglomerateNumber: " + conglomerateNumber + "\n" +
				"td: " + ((td == null) ? "null" : td.toString()) + "\n" +
// GemStone changes BEGIN
				"onlyIfExists: " + this.onlyIfExists + '\n' +
// GemStone changes END
				"dropBehavior: " + "\n" + dropBehavior + "\n";
		}
		else
		{
			return "";
		}
	}

	public String statementToString()
	{
		return "DROP TABLE";
	}

	/**
	 * Bind this LockTableNode.  This means looking up the table,
	 * verifying it exists and getting the heap conglomerate number.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void bindStatement() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();

// GemStone changes BEGIN
		if (this.onlyIfExists) {
		  try {
		    td = getTableDescriptor();
		  } catch (StandardException se) {
		    if (SQLState.LANG_OBJECT_DOES_NOT_EXIST.equals(
		        se.getMessageId())) {
		      td = null;
		      sd = getSchemaDescriptor();
		      return;
		    }
		    else if (SQLState.LANG_SCHEMA_DOES_NOT_EXIST.equals(
		        se.getMessageId())) {
		      td = null;
		      sd = null;
		      return;
		    }
		    else {
		      throw se;
		    }
		  }
		}
		else {
		  td = getTableDescriptor();
		}
		sd = getSchemaDescriptor(td.getTableType() != TableDescriptor.GLOBAL_TEMPORARY_TABLE_TYPE);
		/* (original code)
		td = getTableDescriptor();
		*/
// GemStone changes END

		conglomerateNumber = td.getHeapConglomerateId();

		/* Get the base conglomerate descriptor */
		ConglomerateDescriptor cd = td.getConglomerateDescriptor(conglomerateNumber);

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(td);
		cc.createDependency(cd);
	}

	/**
	 * Return true if the node references SESSION schema tables (temporary or permanent)
	 *
	 * @return	true if references SESSION schema tables, else false
	 *
	 * @exception StandardException		Thrown on error
	 */
	public boolean referencesSessionSchema()
		throws StandardException
	{
		//If table being dropped is in SESSION schema, then return true. 
// GemStone changes BEGIN
		return isSessionSchema(sd);
		/* (original code)
		return isSessionSchema(td.getSchemaDescriptor());
		*/
// GemStone changes END
	}

	// inherit generate() method from DDLStatementNode


	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
		return	getGenericConstantActionFactory().getDropTableConstantAction( getFullName(),
											 getRelativeName(),
// GemStone changes BEGIN
											 sd,
											 /* (original code)
											 getSchemaDescriptor(),
											 */
// GemStone changes END
											 conglomerateNumber,
// GemStone changes BEGIN
											 td != null
											   ? td.getUUID()
											   : null,
											 this.onlyIfExists,
											 /* (original code)
											 td.getUUID(),
											 */
// GemStone changes END
											 dropBehavior);
	}
}
