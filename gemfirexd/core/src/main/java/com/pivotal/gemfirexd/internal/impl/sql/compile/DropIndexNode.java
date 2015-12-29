/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.DropIndexNode

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
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConglomerateDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ConstraintDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * A DropIndexNode is the root of a QueryTree that represents a DROP INDEX
 * statement.
 *
 */

public class DropIndexNode extends DDLStatementNode
{
	private ConglomerateDescriptor	cd;
	private TableDescriptor			td;
// GemStone changes BEGIN
	private boolean onlyIfExists;
// GemStone changes END
	
	public String statementToString()
	{
		return "DROP INDEX";
	}

// GemStone changes BEGIN
	public void init(Object indexName, Object onlyIfExists) throws StandardException{
	  super.init(indexName);
	  this.onlyIfExists = ((Boolean)onlyIfExists).booleanValue();
	}
// GemStone changes END
	
	/**
	 * Bind this DropIndexNode.  This means looking up the index,
	 * verifying it exists and getting the conglomerate number.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		DataDictionary			dd = getDataDictionary();
		SchemaDescriptor		sd;

// GemStone changes BEGIN		
		try{
		  sd = getSchemaDescriptor();  
		}
		catch(StandardException e){
		  if(e.getSQLState().equals(SQLState.LANG_SCHEMA_DOES_NOT_EXIST) && onlyIfExists){
		    return;
		  }
		  else{
		    throw e;
		  }
		}
// GemStone changes END               
		
		  /*
		   * Original code
		   * sd = getSchemaDescriptor();
		   */
		  
		if (sd.getUUID() != null) 
			cd = dd.getConglomerateDescriptor(getRelativeName(), sd, false);

		if (cd == null)
		{
// GemStone changes BEGIN		  
		  if(onlyIfExists){
		    return;
		  }
// GemStone changes END		  
		    throw StandardException.newException(SQLState.LANG_INDEX_NOT_FOUND, getFullName());
		}

		/* Get the table descriptor */
		td = getTableDescriptor(cd.getTableID());

		/* Drop index is not allowed on an index backing a constraint -
		 * user must drop the constraint, which will drop the index.
		 * Drop constraint drops the constraint before the index,
		 * so it's okay to drop a backing index if we can't find its
		 * ConstraintDescriptor.
		 */
		if (cd.isConstraint())
		{
			ConstraintDescriptor conDesc;
			String constraintName;

			conDesc = dd.getConstraintDescriptor(td, cd.getUUID());
			if (conDesc != null)
			{
				constraintName = conDesc.getConstraintName();
				throw StandardException.newException(SQLState.LANG_CANT_DROP_BACKING_INDEX, 
										getFullName(), constraintName);
			}
		}

		/* Statement is dependent on the TableDescriptor and ConglomerateDescriptor */
		cc.createDependency(td);
		cc.createDependency(cd);
	}

	// inherit generate() method from DDLStatementNode

	/**
	 * Create the Constant information that will drive the guts of Execution.
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public ConstantAction	makeConstantAction() throws StandardException
	{
// GemStone changes BEGIN    	  
          SchemaDescriptor sd = null;
          try{
                 sd = getSchemaDescriptor();
          }
          catch(StandardException e){
            if(!(e.getSQLState().equals(SQLState.LANG_SCHEMA_DOES_NOT_EXIST) && this.onlyIfExists)){
              throw e;
            }
          }
// GemStone changes END                                                                                  
		return	getGenericConstantActionFactory().getDropIndexConstantAction( getFullName(),
											 getRelativeName(),
											 getRelativeName(),
// GemStone changes BEGIN                                                                                        
											 sd == null ? null : getSchemaDescriptor().getSchemaName(),
											 td == null ? null :td.getUUID(),
											 td == null ? 0L : td.getHeapConglomerateId()
											 ,onlyIfExists
// GemStone changes END											 
											 );
	}
}
