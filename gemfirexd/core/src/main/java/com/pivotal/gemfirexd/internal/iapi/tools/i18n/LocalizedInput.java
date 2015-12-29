/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.tools.i18n.LocalizedInput

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.io.IOException;

public class LocalizedInput extends BufferedReader{
	private InputStream in;
// GemStone changes BEGIN
	// jline based console reader to be used for interactive sessions
	private Object consoleReader;

	private final String encoding;

	public final void setConsoleReader(final Object reader) {
	  this.consoleReader = reader;
	}

	public final Object getConsoleReader() {
	  return this.consoleReader;
	}

	public final String getEncoding() {
	  return this.encoding;
	}
// GemStone changes END
	public LocalizedInput(InputStream i){
		super(new InputStreamReader(i));
		this.in = i;
// GemStone changes BEGIN
		this.encoding = null;
// GemStone changes END
	}

	LocalizedInput(InputStream i, String encode) throws UnsupportedEncodingException{
		super(new InputStreamReader(i,encode));
		this.in = i;
// GemStone changes BEGIN
		this.encoding = encode;
// GemStone changes END
	}
	public boolean isStandardInput(){
		return (in == System.in);
	}
	public void close() throws IOException {
		if (!isStandardInput()) {
			super.close();
		}
	}

}
