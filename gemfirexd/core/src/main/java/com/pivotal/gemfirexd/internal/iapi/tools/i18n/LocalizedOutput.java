/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedOutput

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
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
package com.pivotal.gemfirexd.internal.iapi.tools.i18n;

import java.io.PrintWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

public class LocalizedOutput extends PrintWriter {
	private OutputStream out;
	public LocalizedOutput(OutputStream o){
		super(new OutputStreamWriter(o), true);
		out = o;
	}
	LocalizedOutput(OutputStream o, String enc) throws UnsupportedEncodingException {
		super(new OutputStreamWriter(o, enc), true);
		out = o;
// GemStone changes BEGIN
		this.enc = enc;
// GemStone changes END
	}
	public boolean isStandardOutput(){
		return (out == System.out);
	}
	public void close() {
		if (!isStandardOutput()) {
			super.close();
		}
	}
// GemStone changes BEGIN
	private String enc;

	public LocalizedOutput(java.io.Writer writer) {
	  super(writer, true);
	  this.out = null;
	}

	public OutputStream getOutputStream() {
	  return this.out;
	}

	public String getEncoding() {
	  return this.enc;
	}
// GemStone changes END
}
