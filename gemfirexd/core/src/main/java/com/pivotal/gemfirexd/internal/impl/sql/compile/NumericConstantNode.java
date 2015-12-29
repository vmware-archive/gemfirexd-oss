/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.NumericConstantNode

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

package	com.pivotal.gemfirexd.internal.impl.sql.compile;








import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.info.JVMInfo;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.C_NodeTypes;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeUtilities;
import com.pivotal.gemfirexd.internal.iapi.types.NumberDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;

import java.sql.Types;

public final class NumericConstantNode extends ConstantNode
{
	/**
	 * Initializer for a typed null node
	 *
	 * @param arg1	The TypeId for the type of node OR An object containing the value of the constant.
	 *
	 * @exception StandardException
	 */
	public void init(Object arg1)
		throws StandardException
	{
		int precision = 0, scal = 0, maxwidth = 0;
		Boolean isNullable;
		boolean valueInP; // value in Predicate-- if TRUE a value was passed in
		TypeId  typeId = null;
		int typeid = 0;

		if (arg1 instanceof TypeId)
		{
			typeId = (TypeId)arg1;
			isNullable = Boolean.TRUE;
			valueInP = false;
			maxwidth = 0;
		}

		else	
		{
			isNullable = Boolean.FALSE;
			valueInP = true;
		}

		
		switch (getNodeType())
		{
		case C_NodeTypes.TINYINT_CONSTANT_NODE:
			precision = TypeId.SMALLINT_PRECISION;
			scal = TypeId.SMALLINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.SMALLINT_MAXWIDTH;
				typeid = Types.TINYINT;
				setValue(getDataValueFactory().getDataValue((Byte) arg1));
			} 
			break;

		case C_NodeTypes.INT_CONSTANT_NODE:
			precision = TypeId.INT_PRECISION;
			scal = TypeId.INT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.INT_MAXWIDTH;
				typeid = Types.INTEGER;
				setValue(new SQLInteger((Integer) arg1));
			}
			break;

		case C_NodeTypes.SMALLINT_CONSTANT_NODE:
			precision = TypeId.SMALLINT_PRECISION;
			scal = TypeId.SMALLINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.SMALLINT_MAXWIDTH;
				typeid = Types.SMALLINT;
				setValue(getDataValueFactory().getDataValue((Short) arg1));
			}
			break;

		case C_NodeTypes.LONGINT_CONSTANT_NODE:
			precision = TypeId.LONGINT_PRECISION;
			scal = TypeId.LONGINT_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.LONGINT_MAXWIDTH;
				typeid = Types.BIGINT;
				setValue(getDataValueFactory().getDataValue((Long) arg1));
			}
			break;
			
		case C_NodeTypes.DECIMAL_CONSTANT_NODE:
			if (valueInP)
			{

				NumberDataValue constantDecimal = getDataValueFactory().getDecimalDataValue((String) arg1);

				typeid = Types.DECIMAL;
				precision = constantDecimal.getDecimalValuePrecision();
				scal = constantDecimal.getDecimalValueScale();
				/* be consistent with our convention on maxwidth, see also
				 * exactNumericType(), otherwise we get format problem, b 3923
				 */
				maxwidth = DataTypeUtilities.computeMaxWidth(precision, scal);
				setValue(constantDecimal);
			}
			else
			{
				precision = TypeCompiler.DEFAULT_DECIMAL_PRECISION;
				scal = TypeCompiler.DEFAULT_DECIMAL_SCALE;
				maxwidth = TypeId.DECIMAL_MAXWIDTH;
			}
			break;
												   
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			precision = TypeId.DOUBLE_PRECISION;
			scal = TypeId.DOUBLE_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.DOUBLE_MAXWIDTH;
				typeid = Types.DOUBLE;
				setValue(getDataValueFactory().getDataValue((Double) arg1));
			}
			break;

		case C_NodeTypes.FLOAT_CONSTANT_NODE:
			precision = TypeId.REAL_PRECISION;
			scal = TypeId.REAL_SCALE;
			if (valueInP)
			{
				maxwidth = TypeId.REAL_MAXWIDTH;
				typeid = Types.REAL;
				setValue(
					getDataValueFactory().getDataValue((Float) arg1));
			}
			break;
			
		default:
			if (SanityManager.DEBUG)
			{
				// we should never really come here-- when the class is created
				// it should have the correct nodeType set.
				SanityManager.THROWASSERT(
								"Unexpected nodeType = " + getNodeType());
			}
			break;
		}
		
		setType(
				   (typeId != null) ?  typeId :
				     TypeId.getBuiltInTypeId(typeid),

				   precision, 
				   scal, 
				   isNullable.booleanValue(), 
				   maxwidth);
	}

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
	 * @exception StandardException		Thrown on error
	 */
	public Object getConstantValueAsObject()
		throws StandardException
	{
		return value.getObject();
	}

		/**
	 * This generates the proper constant.  It is implemented
	 * by every specific constant node (e.g. IntConstantNode).
	 *
	 * @param acb	The ExpressionClassBuilder for the class being built
	 * @param mb	The method the expression will go into
	 *
	 * @exception StandardException		Thrown on error
	 */
	void generateConstant(ExpressionClassBuilder acb, MethodBuilder mb)
		throws StandardException
	{
		switch (getNodeType())
		{
		case C_NodeTypes.INT_CONSTANT_NODE:
			mb.push(value.getInt());
			break;
		case C_NodeTypes.TINYINT_CONSTANT_NODE:
			mb.push(value.getByte());
			break;
		case C_NodeTypes.SMALLINT_CONSTANT_NODE:
			mb.push(value.getShort());
			break;
		case C_NodeTypes.DECIMAL_CONSTANT_NODE:
			// No java.math.BigDecimal class in J2ME so the constant
			// from the input SQL is handled directly as a String.
			if (!JVMInfo.J2ME)
				mb.pushNewStart("java.math.BigDecimal");
			mb.push(value.getString());
			if (!JVMInfo.J2ME)
				mb.pushNewComplete(1);
			break;
		case C_NodeTypes.DOUBLE_CONSTANT_NODE:
			mb.push(value.getDouble());
			break;
		case C_NodeTypes.FLOAT_CONSTANT_NODE:
			mb.push(value.getFloat());
			break;
		case C_NodeTypes.LONGINT_CONSTANT_NODE:
			mb.push(value.getLong());
			break;
		default:
			if (SanityManager.DEBUG)
			{
				// we should never really come here-- when the class is created
				// it should have the correct nodeType set.
				SanityManager.THROWASSERT(
						  "Unexpected nodeType = " + getNodeType());
			}
		}	
	}
}		
