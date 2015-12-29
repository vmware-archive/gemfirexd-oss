/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.reflect.ReflectMethod

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


import com.gemstone.gemfire.SystemFailure;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;

import java.lang.reflect.Method;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

class ReflectMethod implements GeneratedMethod {

	private final Method	realMethod;

	ReflectMethod(Method m) {
		super();
		realMethod = m;
	}

	public Object invoke(Object ref)
		throws StandardException {

		Throwable t;

		try {
			return realMethod.invoke(ref, (Object[])null);

		} catch (IllegalAccessException iae) {

			t = iae;

		} catch (IllegalArgumentException iae2) {

			t = iae2;

		} catch (InvocationTargetException ite) {

			t = ite.getTargetException();
			if (t instanceof StandardException)
				throw (StandardException) t;
// GemStone changes BEGIN
			if (t instanceof SQLException) {
			  throw Misc.wrapSQLException((SQLException)t, t);
			}
			Error err;
			if (t instanceof Error && SystemFailure
			    .isJVMFailureError(err = (Error)t)) {
			  SystemFailure.initiateFailure(err);
			  // If this ever returns, rethrow the error. We're
			  // poisoned now, so don't let this thread continue.
			  throw err;
			}
// GemStone changes END
		}
		
		throw StandardException.unexpectedUserException(t);
	}


}
