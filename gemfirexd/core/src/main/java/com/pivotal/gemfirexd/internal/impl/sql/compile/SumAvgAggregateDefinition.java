/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.SumAvgAggregateDefinition

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

package com.pivotal.gemfirexd.internal.impl.sql.compile;






import java.sql.Types;

import com.pivotal.gemfirexd.internal.catalog.TypeDescriptor;
import com.pivotal.gemfirexd.internal.catalog.types.UserDefinedTypeIdImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompiler;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.TypeCompilerFactory;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.impl.sql.execute.AvgAggregator;
import com.pivotal.gemfirexd.internal.impl.sql.execute.SumAggregator;
import com.pivotal.gemfirexd.internal.shared.common.StoredFormatIds;

/**
 * Defintion for the SUM()/AVG() aggregates.
 *
 */
public class SumAvgAggregateDefinition
		implements AggregateDefinition 
{
	private boolean isSum;
        //GemStone changes BEGIN
        private boolean distinct = false;
        //GemStone changes END
	/**
	 * Niladic constructor.  Does nothing.  For ease
	 * Of use, only.
	 */
	public SumAvgAggregateDefinition() { super(); }

	/**
	 * Determines the result datatype.  Accept NumberDataValues
	 * only.  
	 * <P>
	 * <I>Note</I>: In the future you should be able to do
	 * a sum user data types.  One option would be to run
	 * sum on anything that implements plus().  In which
	 * case avg() would need divide().
	 *
	 * @param inputType	the input type, either a user type or a java.lang object
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	/// Gemstone changes : Added isAvg parameter to getAggregator method.
	public final DataTypeDescriptor	getAggregator(DataTypeDescriptor inputType, 
				StringBuilder aggregatorClass, boolean isAvg) 
	{
		try
		{
		        //GemStone changes END 
		        if (isAvg) {
		          inputType = promoteDataTypeDescriptor(inputType);
		        }
		        //GemStone changes END
		        TypeId compType = inputType.getTypeId();
	                //TypeId compType = promoteTypeId(inputType.getTypeId());
		  	CompilerContext cc = (CompilerContext)
				ContextService.getContext(CompilerContext.CONTEXT_ID);
			TypeCompilerFactory tcf = cc.getTypeCompilerFactory();
			TypeCompiler tc = tcf.getTypeCompiler(compType);
		
			/*
			** If the class implements NumberDataValue, then we
			** are in business.  Return type is same as input
			** type.
			*/
			if (compType.isNumericTypeId())
			{
				aggregatorClass.append(getAggregatorClassName());

				DataTypeDescriptor outDts = tc.resolveArithmeticOperation( 
// GemStone changes BEGIN
				    inputType, inputType,
				    isAvg ? TypeCompiler.AVG_OP : getOperator());
			/* (original code)
                        inputType, inputType, getOperator());
                        */
				
                //lets make room for distinct aggregate column value transportation.
                // Also, If all tables are replicated, no special processing on Data Node
                  if( !cc.createQueryInfo() && !cc.allTablesAreReplicatedOnRemote() && distinct) {
                    TypeId cti = TypeId.getUserDefinedTypeId(
                              com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet.class.getName(), inputType, false);
                    outDts = new DataTypeDescriptor(cti, false);
                  }
                  
// GemStone changes END
				/*
				** SUM and AVG may return null
				*/
				return outDts.getNullabilityType(true);
			}
		}
		catch (StandardException e)
		{
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT("Unexpected exception", e);
			}
		}

		return null;
  }

// GemStone changes BEGIN
  protected final DataTypeDescriptor promoteDataTypeDescriptor(
      DataTypeDescriptor value) throws StandardException {
    switch (value.getTypeId().getTypeFormatId()) {
      case StoredFormatIds.INT_TYPE_ID:
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(
            java.sql.Types.BIGINT, false);
      case StoredFormatIds.SMALLINT_TYPE_ID:
      case StoredFormatIds.TINYINT_TYPE_ID:
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(
            java.sql.Types.INTEGER, false);
      case StoredFormatIds.REAL_TYPE_ID:
        return DataTypeDescriptor.getBuiltInDataTypeDescriptor(
            java.sql.Types.DOUBLE, false);
      default:
        return value;
    }
  }
//GemStone changes END
	/**
	 * Return the aggregator class.  
	 *
	 * @return SumAggregator.CLASS_NAME/AvgAggregator.CLASS_NAME
	 */
	private String getAggregatorClassName()
	{
		if ( isSum )
				return ClassName.SumAggregator;
		else
				return ClassName.AvgAggregator;
	}

	/**
	 * Return the arithmetic operator corresponding
	 * to this operation.
	 *
	 * @return TypeCompiler.SUM_OP /TypeCompiler.AVG_OP
	 */
	protected String getOperator()
	{
		if ( isSum )
				return TypeCompiler.SUM_OP;
		else
				return TypeCompiler.AVG_OP;
	}

	/**
	 * This is set by the parser.
	 */
	public final void setSumOrAvg(boolean isSum)
	{
		this.isSum = isSum;
	}

        //GemStone changes BEGIN
        public void markDistinct() {
          distinct = true;
        }
        //GemStone changes END
}
