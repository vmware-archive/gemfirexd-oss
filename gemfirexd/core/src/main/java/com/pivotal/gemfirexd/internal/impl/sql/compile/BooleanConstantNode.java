/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.BooleanConstantNode

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
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Optimizable;
import com.pivotal.gemfirexd.internal.iapi.types.BooleanDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;

import java.sql.Types;

public final class BooleanConstantNode extends ConstantNode
{
	/* Cache actual value to save overhead and
	 * throws clauses.
	 */
	boolean booleanValue;
	boolean unknownValue;

	/**
	 * Initializer for a BooleanConstantNode.
	 *
	 * @param arg1	A boolean containing the value of the constant OR The TypeId for the type of the node
	 *
	 * @exception StandardException
	 */
	public void init(
					Object arg1)
		throws StandardException
	{
		/*
		** RESOLVE: The length is fixed at 1, even for nulls.
		** Is that OK?
		*/

		if (arg1 instanceof Boolean)
		{
			/* Fill in the type information in the parent ValueNode */
			super.init(TypeId.BOOLEAN_ID,
			 Boolean.FALSE,
			 ReuseFactory.getInteger(1));

			booleanValue = ((Boolean) arg1).booleanValue();
			super.setValue(getDataValueFactory().getDataValue(booleanValue));
		}
		else
		{
			super.init(
				arg1,
				Boolean.TRUE,
				ReuseFactory.getInteger(0));
			unknownValue = true;
		}
	}

	/**
	 * Return the value from this BooleanConstantNode
	 *
	 * @return	The value of this BooleanConstantNode.
	 *
	 */

	//public boolean	getBoolean()
	//{
	//	return booleanValue;
	//}

	/**
	 * Return the length
	 *
	 * @return	The length of the value this node represents
	 *
	 * @exception StandardException		Thrown on error
	 */

	//public int	getLength() throws StandardException
	//{
	//	return value.getLength();
	//}

	/**
	 * Return an Object representing the bind time value of this
	 * expression tree.  If the expression tree does not evaluate to
	 * a constant at bind time then we return null.
	 * This is useful for bind time resolution of VTIs.
	 * RESOLVE: What do we do for primitives?
	 *
	 * @return	An Object representing the bind time value of this expression tree.
	 *			(null if not a bind time constant.)
	 *
	 */
	public Object getConstantValueAsObject()
	{
		return booleanValue ? Boolean.TRUE : Boolean.FALSE;
	}

	/**
	 * Return the value as a string.
	 *
	 * @return The value as a string.
	 *
	 */
	String getValueAsString()
	{
		if (booleanValue)
		{
			return "true";
		}
		else
		{
			return "false";
		}
	}
//      GemStone changes BEGIN
// changed to public from package-visibility
	/**
	 * Does this represent a true constant.
	 *
	 * @return Whether or not this node represents a true constant.
	 */
	public boolean isBooleanTrue()
	{
		return (booleanValue && !unknownValue);
	}

	/**
	 * Does this represent a false constant.
	 *
	 * @return Whether or not this node represents a false constant.
	 */
	public boolean isBooleanFalse()
	{
		return (!booleanValue && !unknownValue);
	}
//      GemStone changes END
	/**
	 * The default selectivity for value nodes is 50%.  This is overridden
	 * in specific cases, such as the RelationalOperators.
	 */
	public double selectivity(Optimizable optTable)
	{
		if (isBooleanTrue())
		{
			return 1.0;
		}
		else
		{
			return 0.0;
		}
	}

	/**
	 * Eliminate NotNodes in the current query block.  We traverse the tree, 
	 * inverting ANDs and ORs and eliminating NOTs as we go.  We stop at 
	 * ComparisonOperators and boolean expressions.  We invert 
	 * ComparisonOperators and replace boolean expressions with 
	 * boolean expression = false.
	 * NOTE: Since we do not recurse under ComparisonOperators, there
	 * still could be NotNodes left in the tree.
	 *
	 * @param	underNotNode		Whether or not we are under a NotNode.
	 *							
	 *
	 * @return		The modified expression
	 *
	 */
//Gemstone changes Begin	
public	ValueNode eliminateNots(boolean underNotNode)
//Gemstone changes End	
	{
		if (! underNotNode)
		{
			return this;
		}

		booleanValue = !booleanValue;
		super.setValue(getDataValueFactory().getDataValue(booleanValue));

		return this;
	}

	/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the code to place the code
	 *
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
	{
		mb.push(booleanValue);
	}

	/**
	 * Set the value in this ConstantNode.
	 */
	public void setValue(DataValueDescriptor value)
	{
		super.setValue( value);
        unknownValue = true;
        try
        {
            if( value != null && value.isNotNull().getBoolean())
            {
                booleanValue = value.getBoolean();
                unknownValue = false;
            }
        }
        catch( StandardException se){}
	} // end of setValue
}
