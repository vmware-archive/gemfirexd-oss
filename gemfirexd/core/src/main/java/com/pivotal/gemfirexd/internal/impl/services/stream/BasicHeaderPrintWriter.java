/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.services.stream.BasicHeaderPrintWriter

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

package com.pivotal.gemfirexd.internal.impl.services.stream;

import com.pivotal.gemfirexd.internal.iapi.services.stream.HeaderPrintWriter;
import com.pivotal.gemfirexd.internal.iapi.services.stream.PrintWriterGetHeader;

import java.io.PrintWriter;
import java.io.Writer;
import java.io.OutputStream;

/**
 * Basic class to print lines with headers. 
 * <p>
 *
 * STUB: Should include code to emit a new line before a header
 *			which is not the first thing on the line.
 *
 */
class BasicHeaderPrintWriter 
	extends PrintWriter
	implements HeaderPrintWriter
{

	private final PrintWriterGetHeader headerGetter;
	private final boolean canClose;
	private final String name;

	// constructors

	/**
	 * the constructor sets up the HeaderPrintWriter. 
	 * <p>
	 * @param writeTo       Where to write to.
	 * @param headerGetter	Object to get headers for output lines.
	 * @param canClose      If true, {@link #complete} will also close writeTo
	 * @param streamName    Name of writeTo, e.g. a file name
	 *
	 * @see	PrintWriterGetHeader
	 */
	BasicHeaderPrintWriter(OutputStream writeTo,
			PrintWriterGetHeader headerGetter,  boolean canClose, String streamName){
		super(writeTo, true);
		this.headerGetter = headerGetter;
		this.canClose = canClose;
		this.name = streamName;
	}

	/**
	 * the constructor sets up the HeaderPrintWriter. 
	 * <p>
	 * @param writeTo       Where to write to.
	 * @param headerGetter	Object to get headers for output lines.
	 * @param canClose      If true, {@link #complete} will also close writeTo
	 * @param writerName    Name of writeTo, e.g. a file name
	 *
	 * @see	PrintWriterGetHeader
	 */
	BasicHeaderPrintWriter(Writer writeTo,
			PrintWriterGetHeader headerGetter, boolean canClose, String writerName){
		super(writeTo, true);
		this.headerGetter = headerGetter;
		this.canClose = canClose;
		this.name = writerName;
	}

	/*
	 * HeaderPrintWriter interface (partial; remaining methods
	 * come from the PrintWriter supertype).
	 */
	public synchronized void printlnWithHeader(String message)
	{ 
		print(headerGetter.getHeader());
		println(message);
	}

	public PrintWriterGetHeader getHeader()
	{
		return headerGetter;
	}

	public PrintWriter getPrintWriter(){
		return this;
	}

	public String getName(){
		return name;
	}

	/**
	 * Flushes stream, and optionally also closes it if constructed
	 * with canClose equal to true.
	 */

	void complete() {
		flush();
		if (canClose) {
			close();
		}
	}
// GemStone changes BEGIN

	public int getLogSeverityLevel() {
	  return (com.pivotal.gemfirexd.internal.iapi.services.sanity
	      .SanityManager.DEBUG ? 0 : com.pivotal.gemfirexd.internal.iapi.error.ExceptionSeverity.SESSION_SEVERITY);
	}

	public boolean printStackTrace(Throwable error, int logSeverityLevel) {
	  return true;
	}
// GemStone changes END
}

