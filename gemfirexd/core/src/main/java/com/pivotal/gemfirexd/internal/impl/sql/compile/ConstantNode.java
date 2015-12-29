/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.ConstantNode

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








//GemStone changes BEGIN
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ConstantQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfoContext;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.LocalField;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;
//GemStone changes END



import java.util.Vector;

/**
 * ConstantNode holds literal constants as well as nulls.
 * <p>
 * A NULL from the parser may not yet know its type; that
 * must be set during binding, as it is for parameters.
 * <p>
 * the DataValueDescriptor methods want to throw exceptions
 * when they are of the wrong type, but to do that they
 * must check typeId when the value is null, rather than
 * the instanceof check they do for returning a valid value.
 * <p>
 * For code generation, we generate a static field.  Then we set the 
 * field be the proper constant expression (something like <code>
 * getDatavalueFactory().getCharDataValue("hello", ...)) </code>)
 * in the constructor of the generated method.  Ideally
 * we would have just 
 */
// GemStone changes BEGIN
public abstract class ConstantNode extends ValueNode
/* abstract class ConstantNode extends ValueNode */
// GemStone changes END
{
	DataValueDescriptor	value;

	/*
	** In case generateExpression() is called twice (something
	** that probably wont happen but might), we will cache
	** our generated expression and just return a reference
	** to the field that holds our value (instead of having
	** two fields holding the same constant).
	*/

	/**
	 * Initializer for non-numeric types
	 *
	 * @param typeId	The Type ID of the datatype
	 * @param nullable	True means the constant is nullable
	 * @param maximumWidth	The maximum number of bytes in the data value
	 *
	 * @exception StandardException
	 */
	public void init(
			Object typeId,
			Object nullable,
			Object maximumWidth)
		throws StandardException
	{
        setType((TypeId) typeId,
                ((Boolean) nullable).booleanValue(),
                ((Integer) maximumWidth).intValue());

	}

	/**
	 * Constructor for untyped nodes, which contain little information
	 *
	 */
	ConstantNode()
	{
		super();
	}

	/**
	 * Set the value in this ConstantNode.
	 */
	//GemStone changes BEGIN
	//increasing visibility to public so that ParameterizedConstantNode can set value.
	public void setValue(DataValueDescriptor value)
        //GemStone changes END
	{
		this.value = value;
	}

	/**
	  * Get the value in this ConstantNode
	  */
	public DataValueDescriptor	getValue()
	{
		return	value;
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
			return "value: " + value + "\n" +
				super.toString();
		}
		else
		{
			return "";
		}
	}

	/**
	 * Return whether or not this expression tree is cloneable.
	 *
	 * @return boolean	Whether or not this expression tree is cloneable.
	 */
	public boolean isCloneable()
	{
		return true;
	}

	/**
	 * Return a clone of this node.
	 *
	 * @return ValueNode	A clone of this node.
	 *
	 */
	public ValueNode getClone()
	{
		/* All constants can simply be reused */
		return this;
	}

	/**
	 * Bind this expression.  This means binding the sub-expressions,
	 * as well as figuring out what the return type is for this expression.
	 * In this case, there are no sub-expressions, and the return type
	 * is already known, so this is just a stub.
	 *
	 * @param fromList		The FROM list for the query this
	 *				expression is in, for binding columns.
	 * @param subqueryList		The subquery list being built as we find SubqueryNodes
	 * @param aggregateVector	The aggregate vector being built as we find AggregateNodes
	 *
	 * @return	The new top of the expression tree.
	 *
	 * @exception StandardException		Thrown on error. Although this class
	 * doesn't throw this exception, it's subclasses do and hence this method
	 * signature here needs to have throws StandardException 
	 */
	public ValueNode bindExpression(
			FromList fromList, SubqueryList subqueryList,
			Vector	aggregateVector)
	throws StandardException
	{
		/*
		** This has to be here for binding to work, but it doesn't
		** have to do anything, because the datatypes of constant nodes
		** are pre-generated by the parser.
		*/
		return this;
	}

	/**
	 * Return whether or not this expression tree represents a constant expression.
	 *
	 * @return	Whether or not this expression tree represents a constant expression.
	 */
	public boolean isConstantExpression()
	{
		return true;
	}

	/** @see ValueNode#constantExpression */
	public boolean constantExpression(PredicateList whereClause)
	{
		return true;
	}

	/**
	 * For a ConstantNode, we generate the equivalent literal value.
	 * A null is generated as a Null value cast to the type of
	 * the constant node.
	 * The subtypes of ConstantNode generate literal expressions
	 * for non-null values.
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
	public void generateExpression
	(
		ExpressionClassBuilder	acb, 
		MethodBuilder 		mb
	) throws StandardException
	{
		/* Are we generating a SQL null value? */
	    if (isNull())
	    {
			acb.generateNull(mb, getTypeCompiler(), 
					getTypeServices().getCollationType());
		}
		else
		{
			generateConstant(acb, mb);	// ask sub type to give a constant,
										// usually a literal like 'hello'

			acb.generateDataValue(mb, getTypeCompiler(), 
					getTypeServices().getCollationType(), (LocalField) null);
		}
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 * @exception StandardException		Thrown on error
	 */
	abstract void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException;

	/**
	 * Return whether or not this node represents a typed null constant.
	 *
	 */
	boolean isNull()
	{
		return (value == null || value.isNull());
	}

	/**
	 * Return the variant type for the underlying expression.
	 * The variant type can be:
	 *		VARIANT				- variant within a scan
	 *							  (method calls and non-static field access)
	 *		SCAN_INVARIANT		- invariant within a scan
	 *							  (column references from outer tables)
	 *		QUERY_INVARIANT		- invariant within the life of a query
	 *		VARIANT				- immutable
	 *
	 * @return	The variant type for the underlying expression.
	 */
	protected int getOrderableVariantType()
	{
		// Constants are constant for the life of the query
		return Qualifier.CONSTANT;
	}
        
	//GemStone changes BEGIN
	// increasing visibility only for this for ParameterizedConstantNode to also use.
	public boolean isEquivalent(ValueNode o) throws StandardException
        //GemStone changes END
	{
		if (isSameNodeType(o)) {
			ConstantNode other = (ConstantNode)o;
			
			// value can be null which represents a SQL NULL value.
			return ( (other.getValue() == null && getValue() == null) || 
					 (other.getValue() != null && 
							 other.getValue().compare(getValue()) == 0) );
		}
		return false;
	}
//      GemStone changes BEGIN        
         public QueryInfo computeQueryInfo(QueryInfoContext qic) throws StandardException {
           ConstantQueryInfo cqi = new ConstantQueryInfo(this.value);
           cqi.setMaxWidth(this.maxWidth);
           return cqi;           
                  
         }
        
         protected String columnName;
         protected int columnNumber;
         // Caution: This field is not being correctly set for all scenarios. It has been added
         // to handle a particular case of determining maxWidth of SQLVarchar for SingleHop metadata.
         protected int maxWidth;
         
         public void setCorrespondingColumnInfo(String colName, int colNumber, int maxWidth) {
           this.columnName = colName;
           this.columnNumber = colNumber;
           this.maxWidth = maxWidth;
         }
         
         /**
          * Convert this object to a String for special formatting during EXPLAIN.
          *   This is done as part of PROJECT/PROJECT-RESTRICT and FILTER EXPLAIN output
          *
          * @return      This object as a String
          */

         public String printExplainInfo()
         {
           // value's String conversion may throw exception
           // LIKE predicates use > and <= on unprintable characters, for example
           // Use getTraceString and not toString as we don't need the column's datatype, just the value
           try {
             return value.getTraceString();
           }
           catch (StandardException e)
           {
             // Cannot be represented as a string! Pass back placeholder (TODO : change to ? or <?> for clarity)
             return " ";
           }
         }
        
         @Override
        public String ncjGenerateSql() {
          try {
            if (value instanceof SQLChar) {
              return "'" + value.getString() + "'";
            }
            else {
              return value.getString();
            }
          } catch (StandardException e) {
            SanityManager.THROWASSERT(e);
          }
          return null;
        }
//       GemStone changes END

}
