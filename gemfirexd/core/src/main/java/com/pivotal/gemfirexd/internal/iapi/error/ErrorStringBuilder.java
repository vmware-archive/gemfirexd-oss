/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.services.context.ErrorStringBuilder

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

package com.pivotal.gemfirexd.internal.iapi.error;

import com.pivotal.gemfirexd.internal.iapi.services.stream.PrintWriterGetHeader;
// GemStone changes BEGIN
import com.gemstone.gemfire.internal.shared.StringPrintWriter;
import com.pivotal.gemfirexd.internal.shared.common.SharedUtils;
// GemStone changes END

import java.io.PrintWriter;

/**
 * Class used to form error messages.  Primary
 * reason for existence is to allow a way to call
 * printStackTrace() w/o automatically writting
 * to a stream.
 */
public class ErrorStringBuilder 
{
// GemStone changes BEGIN
	private final StringPrintWriter printWriter;
	/* (original code)
	private StringWriter		stringWriter;
	private PrintWriter		printWriter;
	*/
// GemStone changes END
	private PrintWriterGetHeader	headerGetter;

	/**
	** Construct an error string builder
	*/
	public ErrorStringBuilder(PrintWriterGetHeader headerGetter)
	{
		this.headerGetter = headerGetter;
// GemStone changes BEGIN
		this.printWriter = new StringPrintWriter();
		/* (original code)
		this.stringWriter = new StringWriter();
		this.printWriter = new PrintWriter(stringWriter);
		*/
// GemStone changes END
	}

	/**
	** Append an error string 
	**
	** @param s 	the string to append
	*/
	public void append(String s)
	{
		if (headerGetter != null)
			printWriter.print(headerGetter.getHeader());
		printWriter.print(s);
	}


	/**
	** Append an error string with a newline
	**
	** @param s 	the string to append
	*/
	public void appendln(String s)
	{
		if (headerGetter != null)
			printWriter.print(headerGetter.getHeader());
		printWriter.println(s);
	}

	/**
	** Print a stacktrace from the throwable in the error
	** buffer.
	**
	** @param t	the error
	*/
	public void stackTrace(Throwable t)
	{
// GemStone changes BEGIN
	  SharedUtils.printStackTrace(t, this.printWriter, false);
	/* (original code)
		int level = 0;
		while(t != null)
		{
			if (level > 0)	
				printWriter.println("============= begin nested exception, level (" +
									level + ") ===========");

			t.printStackTrace(printWriter);

			if (t instanceof java.sql.SQLException) {
				Throwable next = ((java.sql.SQLException)t).getNextException();
				t = (next == null) ? t.getCause() : next;
			} else {
				t = t.getCause();
			}

			if (level > 0)	
				printWriter.println("============= end nested exception, level (" + 
									level + ") ===========");

			level++;

		}

	*/
// GemStone changes END
	}

	/**
	** Reset the buffer -- truncate it down to nothing.
	**
	*/
	public void reset()
	{
		// Is this the most effecient way to do this?
// GemStone changes BEGIN
		this.printWriter.getBuilder().setLength(0);
		/* (original code)
		stringWriter.getBuffer().setLength(0);
		*/
// GemStone changes END
	}

	/**
	** Get the buffer
	*/
	public StringBuilder get()
	{
// GemStone changes BEGIN
		return this.printWriter.getBuilder();
		/* (original code)
		return stringWriter.getBuffer();
		*/
// GemStone changes END
	}	
}	
