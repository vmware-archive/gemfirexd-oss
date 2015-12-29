/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.services.io.ApplicationObjectInputStream

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

package com.pivotal.gemfirexd.internal.iapi.services.io;

import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassFactory;

import java.io.ObjectStreamClass;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
	An object input stream that implements resolve class in order
	to load the class through the ClassFactory.loadApplicationClass method.
*/
// Gemstone changes BEGIN
public
// Gemstone changes END
class ApplicationObjectInputStream extends ObjectInputStream
    implements ErrorObjectInput
{

	protected ClassFactory cf;
	protected ObjectStreamClass        initialClass;

// Gemstone changes BEGIN
	public
// Gemstone changes END
	ApplicationObjectInputStream(InputStream in, ClassFactory cf)
		throws IOException {
		super(in);
		this.cf = cf;
	}

	protected Class resolveClass(ObjectStreamClass v)
		throws IOException, ClassNotFoundException {

		if (initialClass == null)
			initialClass = v;

		if (cf != null)
			return cf.loadApplicationClass(v);

		throw new ClassNotFoundException(v.getName());
	}

	public String getErrorInfo() {
		if (initialClass == null)
			return "";

		return initialClass.getName() + " (serialVersionUID="
			+ initialClass.getSerialVersionUID() + ")";
	}

	public Exception getNestedException() {
        return null;
	}

}
