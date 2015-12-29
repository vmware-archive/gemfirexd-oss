/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.DropAliasNode

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








import com.pivotal.gemfirexd.internal.catalog.AliasInfo;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.AliasDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ConstantAction;

/**
 * A DropAliasNode  represents a DROP ALIAS statement.
 *
 */

public class DropAliasNode extends DDLStatementNode
{
	private char aliasType;
	private char nameSpace;
// GemStone changes BEGIN	
	private Boolean onlyIfExists;
// GemStone changes END
	/**
	 * Initializer for a DropAliasNode
	 *
	 * @param dropAliasName	The name of the method alias being dropped
	 * @param aliasType				Alias type
	 *
	 * @exception StandardException
	 */
	public void init(Object dropAliasName, Object aliasType
// GemStone changes BEGIN	    
	                ,Object onlyIfExists
// GemStone changes END	                
	                )
				throws StandardException
	{
		TableName dropItem = (TableName) dropAliasName;
		initAndCheck(dropItem);
		this.aliasType = ((Character) aliasType).charValue();
// GemStone changes BEGIN           
		this.onlyIfExists = ((Boolean)onlyIfExists).booleanValue();
// GemStone changes END           
	
		switch (this.aliasType)
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_PROCEDURE_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_FUNCTION_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_SYNONYM_AS_CHAR;
				break;

			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				nameSpace = AliasInfo.ALIAS_NAME_SPACE_UDT_AS_CHAR;
				break;
// GemStone changes BEGIN
			case AliasInfo.ALIAS_TYPE_RESULT_PROCESSOR_AS_CHAR:
			  nameSpace = AliasInfo.ALIAS_NAME_SPACE_RESULT_PROCESSOR_AS_CHAR;
			  break;
// GemStone changes END

			default:
				if (SanityManager.DEBUG)
				{
					SanityManager.THROWASSERT("bad type to DropAliasNode: "+this.aliasType);
				}
		}
	}

	public	char	getAliasType() { return aliasType; }

	public String statementToString()
	{
		return "DROP ".concat(aliasTypeName(aliasType)
// GemStone changes BEGIN		    
		      ) 
		    + (this.onlyIfExists ? " IF EXISTS": ""
// GemStone changes END		      
		      );
	}

	/**
	 * Bind this DropMethodAliasNode.  
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void bindStatement() throws StandardException
	{
		DataDictionary	dataDictionary = getDataDictionary();
		String			aliasName = getRelativeName();

		AliasDescriptor	ad = null;

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
		/*
		 * Original Code
		 * SchemaDescriptor sd = getSchemaDescriptor();
		 */
		
		if (sd.getUUID() != null) {
			ad = dataDictionary.getAliasDescriptor
			                          (sd.getUUID().toString(), aliasName, nameSpace );
		}
		if ( ad == null)
		{
// GemStone changes BEGIN		  
		  if(onlyIfExists){
		    return;
		  }
		  else{
// GemStone changes END		    
			throw StandardException.newException(SQLState.LANG_OBJECT_DOES_NOT_EXIST, statementToString(), aliasName);
// GemStone changes BEGIN			
		  }
// GemStone changes END		  
		}

		// User cannot drop a system alias
		if (ad.getSystemAlias())
		{
			throw StandardException.newException(SQLState.LANG_CANNOT_DROP_SYSTEM_ALIASES, aliasName);
		}

		// Statement is dependent on the AliasDescriptor
		getCompilerContext().createDependency(ad);
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
		return	getGenericConstantActionFactory().getDropAliasConstantAction(
// GemStone changes BEGIN		    
		    sd, 
// GemStone changes END		    
                    // Original code
                    //,getSchemaDescriptor()
		    getRelativeName(), nameSpace, onlyIfExists.booleanValue());
	}

	/* returns the alias type name given the alias char type */
	private static String aliasTypeName( char actualType)
	{
		String	typeName = null;

		switch ( actualType )
		{
			case AliasInfo.ALIAS_TYPE_PROCEDURE_AS_CHAR:
				typeName = "PROCEDURE";
				break;
			case AliasInfo.ALIAS_TYPE_FUNCTION_AS_CHAR:
				typeName = "FUNCTION";
				break;
			case AliasInfo.ALIAS_TYPE_SYNONYM_AS_CHAR:
				typeName = "SYNONYM";
				break;
			case AliasInfo.ALIAS_TYPE_UDT_AS_CHAR:
				typeName = "TYPE";
				break;
// GemStone changes BEGIN
			case AliasInfo.ALIAS_TYPE_RESULT_PROCESSOR_AS_CHAR:
			  typeName = "PROCEDURE RESULT PROCESSOR";
			  break;
// GemStone changes END
		}
		return typeName;
	}
}
