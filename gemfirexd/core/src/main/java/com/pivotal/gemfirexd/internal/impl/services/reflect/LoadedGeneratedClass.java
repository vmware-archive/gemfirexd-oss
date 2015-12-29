/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.reflect.LoadedGeneratedClass

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

package com.pivotal.gemfirexd.internal.impl.services.reflect;




import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.Context;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassInfo;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedByteCode;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedClass;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;


public abstract class LoadedGeneratedClass
	implements GeneratedClass
{

	/*
	** Fields
	*/

	private final ClassInfo	ci;
	private final int classLoaderVersion;

	/*
	**	Constructor
	*/

	public LoadedGeneratedClass(ClassFactory cf, Class jvmClass) {
		ci = new ClassInfo(jvmClass);
		classLoaderVersion = cf.getClassLoaderVersion();
	}

	/*
	** Public methods from Generated Class
	*/

	public String getName() {
		return ci.getClassName();
	}

// GemStone changes BEGIN
	public Object newInstance(final LanguageConnectionContext context,
	    final boolean addToLCC, final ExecPreparedStatement eps)
	        throws StandardException {
	/* (original code)
	public Object newInstance(Context context, ExecPreparedStatement eps) throws StandardException	{
	*/
// GemStone changes END

		Throwable t;
		try {
			GeneratedByteCode ni =  (GeneratedByteCode) ci.getNewInstance();
			ni.initFromContext(context, addToLCC, eps);
			ni.setGC(this);
			ni.postConstructor();
			return ni;

		} catch (InstantiationException ie) {
			t = ie;
		} catch (IllegalAccessException iae) {
			t = iae;
		} catch (java.lang.reflect.InvocationTargetException ite) {
			t = ite;
		} catch (LinkageError le) {
			t = le;
		}

		throw StandardException.newException(SQLState.GENERATED_CLASS_INSTANCE_ERROR, t, getName());
	}

	public final int getClassLoaderVersion() {
		return classLoaderVersion;
	}

	/*
	** Methods for subclass
	*/
	protected Class getJVMClass() {
		return ci.getClassObject();
	}
}
