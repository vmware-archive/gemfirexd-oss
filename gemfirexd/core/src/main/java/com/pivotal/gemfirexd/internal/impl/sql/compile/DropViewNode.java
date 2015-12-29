/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.DropViewNode

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
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ActivationClassBuilder;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BaseActivation;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * A DropViewNode is the root of a QueryTree that represents a DROP VIEW
 * statement.
 *
 */

public class DropViewNode extends DDLStatementNode
{
//GemStone changes BEGIN       
  private boolean onlyIfExists;
//GemStone changes END

	/**
	 * Initializer for a DropViewNode
	 *
	 * @param dropObjectName	The name of the object being dropped
	 *
	 */

	public void init(Object dropObjectName
// GemStone changes BEGIN           
            ,Object onlyIfExists
//GemStone changes END                 
	    )
		throws StandardException
	{
		initAndCheck(dropObjectName);
// GemStone changes BEGIN           
                this.onlyIfExists = ((Boolean)onlyIfExists).booleanValue();
// GemStone changes END           
		
	}

	public String statementToString()
	{
		return "DROP VIEW";
	}

 	/**
 	 *  Bind the drop view node
 	 *
 	 *
 	 * @exception StandardException		Thrown on error
 	 */
	
	public void bindStatement() throws StandardException
	{
		DataDictionary dd = getDataDictionary();
		CompilerContext cc = getCompilerContext();
		
// GemStone changes BEGIN
		SchemaDescriptor sd = null;
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
				
		TableDescriptor td = dd.getTableDescriptor(getRelativeName(),
// GemStone changes BEGIN
		                        sd,
// GemStone changes END		    
		                        /**
		                         * Original code
		                         *
					 * getSchemaDescriptor(),
					 */
                    getLanguageConnectionContext().getTransactionCompile());
	
		/* 
		 * Statement is dependent on the TableDescriptor 
		 * If td is null, let execution throw the error like
		 * it is before.
		 */
		if (td != null)
		{
			cc.createDependency(td);
		}
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
	  
		return	getGenericConstantActionFactory().getDropViewConstantAction( getFullName(),
											 getRelativeName(),
// GemStone changes BEGIN
											 sd
											 ,onlyIfExists
// GemStone changes END
											 // Original code
											 //getSchemaDescriptor()

											 );
	}
}
