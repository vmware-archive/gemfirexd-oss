/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.DropTriggerNode

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
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TriggerDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * A DropTriggerNode is the root of a QueryTree that represents a DROP TRIGGER
 * statement.
 *
 */
public class DropTriggerNode extends DDLStatementNode
{
	private TableDescriptor td;
// GemStone changes BEGIN
        private boolean onlyIfExists;
// GemStone changes END
	
// GemStone changes BEGIN
        public void init(Object indexName, Object onlyIfExists) throws StandardException{
          super.init(indexName);
          this.onlyIfExists = ((Boolean)onlyIfExists).booleanValue();
        }
// GemStone changes END
	
	
	public String statementToString()
	{
		return "DROP TRIGGER";
	}

	/**
	 * Bind this DropTriggerNode.  This means looking up the trigger,
	 * verifying it exists and getting its table uuid.
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		CompilerContext			cc = getCompilerContext();
		DataDictionary			dd = getDataDictionary();

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
		
		/* Original code
		 *            SchemaDescriptor sd = getSchemaDescriptor();
		 */

		TriggerDescriptor triggerDescriptor = null;
		
		if (sd.getUUID() != null)
			triggerDescriptor = dd.getTriggerDescriptor(getRelativeName(), sd);

		if (triggerDescriptor == null)
 		{
// GemStone changes BEGIN
		  if(onlyIfExists){
		    return;
		  }
		  else
// GemStone changes END		  
			throw StandardException.newException(SQLState.LANG_OBJECT_NOT_FOUND, "TRIGGER", getFullName());
		}

		/* Get the table descriptor */
		td = triggerDescriptor.getTableDescriptor();
		cc.createDependency(td);
		cc.createDependency(triggerDescriptor);
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
            if(!(e.getSQLState().equals(SQLState.LANG_SCHEMA_DOES_NOT_EXIST) && onlyIfExists)){
              throw e;
            }
          }
// GemStone changes END

		return	getGenericConstantActionFactory().getDropTriggerConstantAction(
// GemStone changes BEGIN
		                                                                        sd,
// GemStone changes END		    
		                                                                        /*
		                                                                         * Original code
		                                                                         * getSchemaDescriptor(),
		                                                                         */
											getRelativeName(),
											
											/*
											 * Original code
											 * td.getUUID()
											 */
// GemStone changes BEGIN
											td == null ? null : td.getUUID()
// GemStone changes END											
// GemStone changes BEGIN
											,onlyIfExists
// GemStone changes END											
											
											);
	}
}
