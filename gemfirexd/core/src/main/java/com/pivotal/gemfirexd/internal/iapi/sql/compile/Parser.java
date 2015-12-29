/*

   Derby - Class com.pivotal.gemfirexd.internal.iapi.sql.compile.Parser

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

package com.pivotal.gemfirexd.internal.iapi.sql.compile;


/* Strictly speaking we shouldn't import classes under the impl hierarchy here
 * but this is work in progress.
 * manish - Wed Mar 28 13:05:19 PST 2001
 */
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StatementNode;

/**
 * The Parser interface is intended to work with Jack-generated parsers (now JavaCC).
 * We will specify "STATIC=false" when building Jack parsers - this specifies
 * that the generated classes will not be static, which will allow there to be
 * more than one parser (this is necessary in a multi-threaded server).
 * Non-static parsers do not have to be re-initialized every time they are
 * used (unlike static parsers, for which one must call ReInit() between calls
 * to the parser).
 *
 */


public interface Parser
{

	/**
	 * Parses the given statement and returns a query tree. The query tree
	 * at this point is a simple syntactic translation of the statement.
	 * No binding will have taken place, and no decisions will have been
	 * made regarding processing strategy.
	 *
	 * @param statementSQLText	The Statement to parse.
	 * @param paramDefaults	Parameter defaults
	 * @return	A new QueryTree representing the syntax of the Statement
	 *
	 * @exception StandardException		Thrown on failure
	 */
	public StatementNode	parseStatement(String statementSQLText,
		Object[] paramDefaults) 
		throws StandardException;


	public StatementNode parseStatement(String statementSQLText)
		throws StandardException;
	
        public int matchStatement(String statementSQLText,
            Object[] paramDefaults) throws StandardException;

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	public	String		getSQLtext();
//      GemStone changes BEGIN
	public void setGfxdSubactivationFlag(boolean flag) ;
//      GemStone changes END

}
