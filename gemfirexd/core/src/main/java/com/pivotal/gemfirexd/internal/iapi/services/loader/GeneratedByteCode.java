/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedByteCode

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

package com.pivotal.gemfirexd.internal.iapi.services.loader;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;

/**
	Generated classes must implement this interface.

*/
public interface GeneratedByteCode {

	/**
		Initialize the generated class from a context.
		Called by the class manager just after
		creating the instance of the new class.
	*/
// GemStone changes BEGIN
	public void initFromContext(LanguageConnectionContext lcc,
	    boolean addToLCC, ExecPreparedStatement eps)
	        throws StandardException;
// GemStone changes END

	/**
		Set the Generated Class. Call by the class manager just after
		calling initFromContext.
	*/
	public void setGC(GeneratedClass gc);

	/**
		Called by the class manager just after calling setGC().
	*/
	public void postConstructor() throws StandardException;

	/**
		Get the GeneratedClass object for this object.
	*/
	public GeneratedClass getGC();

	public GeneratedMethod getMethod(String methodName) throws StandardException;


	public Object e0() throws StandardException ; 
	public Object e1() throws StandardException ;
	public Object e2() throws StandardException ;
	public Object e3() throws StandardException ;
	public Object e4() throws StandardException ; 
	public Object e5() throws StandardException ;
	public Object e6() throws StandardException ;
	public Object e7() throws StandardException ;
	public Object e8() throws StandardException ; 
	public Object e9() throws StandardException ;
}
