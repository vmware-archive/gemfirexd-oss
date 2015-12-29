/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.MaterializeSubqueryNode

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
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.LocalField;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ActivationClassBuilder;

/**
 * A MaterializeSubqueryNode is used to replace the nodes for a subquery, to facilitate
 * code generation for materialization if possible.  See beetle 4373 for details.
 *
 */
class MaterializeSubqueryNode extends ResultSetNode
{

	private LocalField lf;

	public MaterializeSubqueryNode(LocalField lf)
	{
		this.lf = lf;
	}

	public void generate(ActivationClassBuilder acb,
						 MethodBuilder mb)
		throws StandardException
	{
		acb.pushThisAsActivation(mb);
		mb.getField(lf);
		mb.callMethod(VMOpcode.INVOKEVIRTUAL, ClassName.BaseActivation, "materializeResultSetIfPossible", ClassName.NoPutResultSet, 1);
	}

	void decrementLevel(int decrement)
	{
	}
	

//GemStone changes END
}
