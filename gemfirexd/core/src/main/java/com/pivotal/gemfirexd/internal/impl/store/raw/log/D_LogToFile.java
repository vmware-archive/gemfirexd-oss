/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.store.raw.log.D_LogToFile

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

package com.pivotal.gemfirexd.internal.impl.store.raw.log;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.diag.DiagnosticUtil;
import com.pivotal.gemfirexd.internal.iapi.services.diag.Diagnosticable;
import com.pivotal.gemfirexd.internal.iapi.services.diag.DiagnosticableGeneric;
import com.pivotal.gemfirexd.internal.impl.store.raw.log.LogCounter;

import java.util.Enumeration;

public class D_LogToFile
extends DiagnosticableGeneric
{

	/**
	  @exception StandardException Oops.
	  @see Diagnosticable#diag
	  */
    public String diag()
 		 throws StandardException
    {
		LogToFile ltf = (LogToFile)diag_object;
		StringBuilder r = new StringBuilder();
		r.append("LogToFile: \n");
		r.append("    Directory: "+ltf.dataDirectory+"\n");
		r.append("    endPosition: "+ltf.endPosition()+"\n");
		r.append("    lastFlush(offset): "+ltf.lastFlush+"\n");
		r.append("    logFileNumber: "+ltf.logFileNumber+"\n");
		r.append("    firstLogFileNumber: "+ltf.firstLogFileNumber+"\n");
		return r.toString();
	}
}



