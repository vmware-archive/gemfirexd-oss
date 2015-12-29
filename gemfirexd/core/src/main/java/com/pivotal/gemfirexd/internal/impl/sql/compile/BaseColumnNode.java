/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.BaseColumnNode

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
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Row;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;

/**
 * A BaseColumnNode represents a column in a base table.  The parser generates a
 * BaseColumnNode for each column reference.  A column refercence could be a column in
 * a base table, a column in a view (which could expand into a complex
 * expression), or a column in a subquery in the FROM clause.  By the time
 * we get to code generation, all BaseColumnNodes should stand only for columns
 * in base tables.
 *
 */

public class BaseColumnNode extends ValueNode
{
	private String	columnName;

	/*
	** This is the user-specified table name.  It will be null if the
	** user specifies a column without a table name.  
	*/
	private TableName	tableName;

	/**
	 * Initializer for when you only have the column name.
	 *
	 * @param columnName	The name of the column being referenced
	 * @param tableName		The qualification for the column
	 * @param dts			DataTypeServices for the column
	 */

	public void init(
							Object columnName,
							Object tableName,
				   			Object dts) throws StandardException
	{
		this.columnName = (String) columnName;
		this.tableName = (TableName) tableName;
		setType((DataTypeDescriptor) dts);
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
			return "columnName: " + columnName + "\n" +
// GemStone changes BEGIN
			    "tableName: " + tableName + '\n' +
			    /* (original code)
				( ( tableName != null) ?
						tableName.toString() :
						"tableName: null\n") +
			    */
// GemStone changes END
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Get the name of this column
	 *
	 * @return	The name of this column
	 */

	public String getColumnName()
	{
		return columnName;
	}

	/**
	 * Get the user-supplied table name of this column.  This will be null
	 * if the user did not supply a name (for example, select a from t).
	 * The method will return B for this example, select b.a from t as b
	 * The method will return T for this example, select t.a from t
	 *
	 * @return	The user-supplied name of this column.  Null if no user-
	 * 		supplied name.
	 */

	public String getTableName()
	{
		return ( ( tableName != null) ? tableName.getTableName() : null );
	}

	/**
	 * Get the user-supplied schema name for this column's table. This will be null
	 * if the user did not supply a name (for example, select t.a from t).
	 * Another example for null return value (for example, select b.a from t as b).
	 * But for following query select app.t.a from t, this will return APP
	 *
	 * @return	The schema name for this column's table
	 */
	public String getSchemaName()
	{
		return ( ( tableName != null) ? tableName.getSchemaName() : null );
	}

	/**
	 * Do the code generation for this node. Should never be called.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 *
	 * @exception StandardException		Thrown on error
	 */

	public void generateExpression(ExpressionClassBuilder acb,
											MethodBuilder mb)
							throws StandardException
	{
		throw StandardException.newException(SQLState.LANG_UNABLE_TO_GENERATE,
			this.nodeHeader());
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *							  (constant expressions)
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType()
	{
		return Qualifier.SCAN_INVARIANT;
	}
        
        // GemStone changes BEGIN
        /**
         * {@inheritDoc}
         */
	protected boolean isEquivalent(ValueNode o)
	{
		if (isSameNodeType(o)) 
		{
			BaseColumnNode other = (BaseColumnNode)o;
			return other.tableName.equals(other.tableName) 
			&& other.columnName.equals(columnName);
		} 
		return false;
	}

        public boolean hasExpression() {
          return false;
        }
        
        @Override
        public String ncjGenerateSql() {
          return columnName;
        }
        // GemStone changes END
}
