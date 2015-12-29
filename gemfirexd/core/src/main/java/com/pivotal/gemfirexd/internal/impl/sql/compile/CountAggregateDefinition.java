/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.CountAggregateDefinition

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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;


/**
 * Defintion for the COUNT()/COUNT(*) aggregates.
 *
 */
public class CountAggregateDefinition 
		implements AggregateDefinition 
{
        //GemStone changes BEGIN
        private boolean distinct = false;
        
        private boolean regionSizeConvertible = false;
        //GemStone changes END
	/**
	 * Niladic constructor.  Does nothing.  For ease
	 * Of use, only.
	 */
	public CountAggregateDefinition() { super(); }

	/**
	 * Determines the result datatype. We can run
	 * count() on anything, and it always returns a
	 * INTEGER (java.lang.Integer).
	 *
	 * @param inputType the input type, either a user type or a java.lang object
	 *
	 * @return the output Class (null if cannot operate on
	 *	value expression of this type.
	 */
	public final DataTypeDescriptor	getAggregator(DataTypeDescriptor inputType,
				StringBuilder aggregatorClass, boolean isAvg)
	throws StandardException
	{
		aggregatorClass.append( ClassName.CountAggregator);
    // GemStone changes BEGIN
		//lets make room for distinct aggregate column value transportation.
		// Also, If all tables are replicated, no special processing on Data Node
                  CompilerContext cc = (CompilerContext)
                          ContextService.getContext(CompilerContext.CONTEXT_ID);
                  if( !cc.createQueryInfo() && !cc.allTablesAreReplicatedOnRemote() && distinct) {
                    TypeId cti = TypeId.getUserDefinedTypeId(
                                        com.pivotal.gemfirexd.internal.engine.sql.compile.types.DVDSet.class.getName(),
                                        DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.INTEGER, false),
                                        false);
                    return new DataTypeDescriptor(cti, false);
                  }
                  
                  // #42682 okay we have count(*), higher up in SelectNode will decide for Region.size().
                  if(inputType == null && !distinct) {
                    regionSizeConvertible = true;
                  }
    // GemStone changes END
		/*
		** COUNT never returns NULL
		*/
		return DataTypeDescriptor.getBuiltInDataTypeDescriptor(java.sql.Types.INTEGER, false);
	}

  // GemStone changes BEGIN
	public void markDistinct() {
	  distinct = true;
	}
	
	public boolean isRegionSizeConvertible() {
	  return regionSizeConvertible;
	}
  // GemStone changes END

}
