/*

   Derby - Class com.ihost.cs.DoubleProperties

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

package com.pivotal.gemfirexd.internal.iapi.util;

import java.util.Properties;
import java.util.Enumeration;

/**
	A properties object that links two independent
	properties together. The read property set is always
	searched first, with the write property set being
	second. But any put() calls are always made directly to
	the write object.

    Only the put(), keys() and getProperty() methods are supported
	by this class.
*/

public final class DoubleProperties extends Properties {

	private final Properties read;
	private final Properties write;

	public DoubleProperties(Properties read, Properties write) {
		this.read = read;
		this.write = write;
	}

	public Object put(Object key, Object value) {
		return write.put(key, value);
	}

	public String getProperty(String key) {

		return read.getProperty(key, write.getProperty(key));
	}

	public String getProperty(String key, String defaultValue) {
		return read.getProperty(key, write.getProperty(key, defaultValue));

	}

	public Enumeration propertyNames() {

		Properties p = new Properties();

		if (write != null) {

			for (Enumeration e = write.propertyNames(); e.hasMoreElements(); ) {
				String key = (String) e.nextElement();
// GemStone changes BEGIN
				String val = write.getProperty(key);
				// allow passing non-string properties
				if (val != null) {
				  p.put(key, val);
				}
				/* p.put(key, write.getProperty(key)); */
// GemStone changes END
			}
		}

		if (read != null) {
			for (Enumeration e = read.propertyNames(); e.hasMoreElements(); ) {
				String key = (String) e.nextElement();
// GemStone changes BEGIN
				String val = read.getProperty(key);
				// allow passing non-string properties
				if (val != null) {
				  p.put(key, val);
				}
				/* p.put(key, read.getProperty(key)); */
// GemStone changes END
			}
		}
		return p.keys();
	}
	// GemStone changes BEGIN
	
	public String toString() {
	  StringBuilder sb = new StringBuilder();
	  sb.append("DoubleProperties@" + System.identityHashCode(this));
	  sb.append("\n read properties ");
	  sb.append(read.toString());
	  sb.append("\n write properties");
	  sb.append(write.toString());
	  return sb.toString();
	}
	// GemStone changes END
}
