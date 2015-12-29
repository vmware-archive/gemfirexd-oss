/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.reflect.ReflectGeneratedClass

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



import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.context.Context;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedByteCode;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Hashtable;

public final class ReflectGeneratedClass extends LoadedGeneratedClass {

	private final Hashtable methodCache;
	private static final GeneratedMethod[] directs;


	private final Class	factoryClass;
	private GCInstanceFactory factory;

	static {
		directs = new GeneratedMethod[10];
		for (int i = 0; i < directs.length; i++) {
			directs[i] = new DirectCall(i);
		}
	}

	public ReflectGeneratedClass(ClassFactory cf, Class jvmClass, Class factoryClass) {
		super(cf, jvmClass);
		methodCache = new Hashtable();
		this.factoryClass = factoryClass;
	}
// GemStone changes BEGIN

	@Override
	public Object newInstance(final LanguageConnectionContext context,
	    final boolean addToLCC, final ExecPreparedStatement eps)
	        throws StandardException	{
// GemStone changes END
		if (factoryClass == null) {
			return super.newInstance(context, addToLCC, eps);
		}

		if (factory == null) {

			Throwable t;
			try {
				factory =  (GCInstanceFactory) factoryClass.newInstance();
				t = null;
			} catch (InstantiationException ie) {
				t = ie;
			} catch (IllegalAccessException iae) {
				t = iae;
			} catch (LinkageError le) {
				t = le;
			}

			if (t != null)
				throw StandardException.newException(SQLState.GENERATED_CLASS_INSTANCE_ERROR, t, getName());
		}

		GeneratedByteCode ni = factory.getNewInstance();
		ni.initFromContext(context, addToLCC, eps);
		ni.setGC(this);
		ni.postConstructor();
		return ni;

	}

	public GeneratedMethod getMethod(String simpleName)
		throws StandardException {

		GeneratedMethod rm = (GeneratedMethod) methodCache.get(simpleName);
		if (rm != null)
			return rm;

		// Only look for methods that take no arguments
		try {
			if ((simpleName.length() == 2) && simpleName.startsWith("e")) {

				int id = ((int) simpleName.charAt(1)) - '0';

				rm = directs[id];


			}
			else
			{
				Method m = getJVMClass().getMethod(simpleName, (Class []) null);
				
				rm = new ReflectMethod(m);
			}
			methodCache.put(simpleName, rm);
			return rm;

		} catch (NoSuchMethodException nsme) {
			throw StandardException.newException(SQLState.GENERATED_CLASS_NO_SUCH_METHOD,
				nsme, getName(), simpleName);
		}
	}
}

class DirectCall implements GeneratedMethod {

	private final int which;

	DirectCall(int which) {

		this.which = which;
	}

	public Object invoke(Object ref)
		throws StandardException {

		try {

			GeneratedByteCode gref = (GeneratedByteCode) ref;
			switch (which) {
			case 0:
				return gref.e0();
			case 1:
				return gref.e1();
			case 2:
				return gref.e2();
			case 3:
				return gref.e3();
			case 4:
				return gref.e4();
			case 5:
				return gref.e5();
			case 6:
				return gref.e6();
			case 7:
				return gref.e7();
			case 8:
				return gref.e8();
			case 9:
				return gref.e9();
			}
			return null;
		} catch (StandardException se) {
			throw se;
		}		
		catch (Throwable t) {
// GemStone changes BEGIN
		        StandardException se = checkForSQLExceptionAndStandardException(t);
		        if (se != null) {
		          throw se;
		        }
// GemStone changes END
			throw StandardException.unexpectedUserException(t);
		}
	}
// GemStone changes BEGIN	
	private StandardException checkForSQLExceptionAndStandardException(Throwable t) {
	  if (t instanceof SQLException) {
            return Misc.wrapSQLException((SQLException)t, t);
          }
	  Throwable innerException = t.getCause();
	  while(innerException != null) {
	    if (innerException instanceof StandardException) {
	      return (StandardException)innerException;
	    }
	    if (innerException instanceof SQLException) {
	      return Misc.wrapSQLException((SQLException)innerException, t);
	    }
	    innerException = innerException.getCause();
	  }
	  return null;
	}
// GemStone changes END
}
