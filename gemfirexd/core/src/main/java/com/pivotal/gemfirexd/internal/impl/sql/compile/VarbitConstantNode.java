/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.VarbitConstantNode

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
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.types.BitDataValue;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.iapi.types.TypeId;
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;


import java.sql.Types;

public final class VarbitConstantNode extends BitConstantNode
{
	/**
	 * Initializer for a VarbitConstantNode.
	 *
	 * @param arg1  The TypeId for the type of the node OR A Bit containing the value of the constant
	 *
	 * @exception StandardException
	 */

	public void init(
						Object arg1)
		throws StandardException
	{
		init(
					arg1,
					Boolean.TRUE,
					ReuseFactory.getInteger(0));

	}
}
