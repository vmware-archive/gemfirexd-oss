/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.sql.compile.ParserImpl

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

package com.pivotal.gemfirexd.internal.impl.sql.compile;


// GemStone changes BEGIN
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.sql.compile.SQLMatcher;
import com.pivotal.gemfirexd.internal.engine.sql.compile.SQLMatcherTokenManager;
// GemStone changes END
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Statement;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Parser;
import com.pivotal.gemfirexd.internal.impl.sql.compile.QueryTreeNode;

public class ParserImpl implements Parser
{
	/*
	** We will use the following constant to pass in to
	** our CharStream.  It is the size of the internal
	** buffers that are used to buffer tokens.  It
	** should be set to what is typically around the
	** largest token that is likely to be hit.  Note
	** that if the size is exceeded, the buffer will
	** automatically be expanded by 2048, so it is ok
	** to choose something that is smaller than the
	** max token supported.
	**
	** Since, JavaCC generates parser and tokenmanagers classes
	** tightly connected, to use another parser or tokenmanager
	** inherit this class, override the following methods
	** to use specific instances:<ul>
	** <li>getTokenManager()</li>
	** <li>getParser()</li>
	** <li>parseGoalProduction(...)</li>
	** </ul>
	**
	*/
	static final int LARGE_TOKEN_SIZE = 128;

        /* Don't ever access these objects directly, call getParser(), and getTokenManager() */
        private SQLParser cachedParser; 
        private SQLMatcher cachedMatcher; 
	protected Object cachedTokenManager;
        protected Object cachedMatcherTokenManager;

	protected CharStream charStream;
        protected com.pivotal.gemfirexd.internal.engine.sql.compile.CharStream matcherCharStream;
        protected String SQLtext;

        protected final CompilerContext cc;
        private  boolean gfxdSubactivationNeeded = false; 

	/**
	 * Constructor for Parser
	 */

	public ParserImpl(CompilerContext cc)
	{
		this.cc = cc;
	}

	public StatementNode parseStatement(String statementSQLText) 
		throws StandardException
	{
		return parseStatement(statementSQLText, (Object[])null);
	}

        /**
	 * Returns a initialized (clean) TokenManager, paired w. the Parser in getParser,
	 * Appropriate for this ParserImpl object.
	 */
        protected Object getTokenManager()
        {
	    /* returned a cached tokenmanager if already exists, otherwise create */
	    SQLParserTokenManager tm = (SQLParserTokenManager) cachedTokenManager;
	    if (tm == null) {
		tm = new SQLParserTokenManager(charStream);
		cachedTokenManager = tm;
	    } else {
		tm.ReInit(charStream);
	    }
	    return tm;
	}

        /**
         * Returns a initialized (clean) TokenManager, paired w. the Parser in getParser,
         * Appropriate for this ParserImpl object.
         */
        protected Object getMatcherTokenManager()
        {
            /* returned a cached tokenmanager if already exists, otherwise create */
            SQLMatcherTokenManager tm = (SQLMatcherTokenManager) cachedTokenManager;
            if (tm == null) {
                tm = new SQLMatcherTokenManager(matcherCharStream);
                cachedTokenManager = tm;
            } else {
                tm.ReInit(matcherCharStream);
            }
            return tm;
        }
        
     /**
	 * new parser, appropriate for the ParserImpl object.
	 */
     private SQLParser getParser()
        {
	    SQLParserTokenManager tm = (SQLParserTokenManager) getTokenManager();
	    /* returned a cached Parser if already exists, otherwise create */
	    SQLParser p = (SQLParser) cachedParser;
	    if (p == null) {
		p = new SQLParser(tm);
		p.setCompilerContext(cc);
		cachedParser = p;
	    } else {
		p.ReInit(tm);
	    }
//          GemStone changes BEGIN
	    p.setGfxdSubactivationFlag(this.gfxdSubactivationNeeded);
//          GemStone changes END
	    return p;
	}

         private SQLMatcher getMatcher()
         {
             SQLMatcherTokenManager tm = (SQLMatcherTokenManager) getMatcherTokenManager();
             /* returned a cached Parser if already exists, otherwise create */
             SQLMatcher m = (SQLMatcher) cachedMatcher;
             if (m == null) {
                 m = new SQLMatcher(tm);
                 m.setCompilerContext(cc);
                 cachedMatcher = m;
             } else {
                 m.ReInit(tm);
             }
             return m;
         }
         
	/**
	 * Parse a statement and return a query tree.  Implements the Parser
	 * interface
	 *
	 * @param statementSQLText	Statement to parse
	 * @param paramDefaults	parameter defaults. Passed around as an array
	 *                      of objects, but is really an array of StorableDataValues
	 * @return	A QueryTree representing the parsed statement
	 *
	 * @exception StandardException	Thrown on error
	 */

	public StatementNode parseStatement(String statementSQLText, Object[] paramDefaults) 
		throws StandardException
	{

		java.io.Reader sqlText = new java.io.StringReader(statementSQLText);

		/* Get a char stream if we don't have one already */
		if (charStream == null)
		{
			charStream = new UCode_CharStream(sqlText, 1, 1, LARGE_TOKEN_SIZE);
		}
		else
		{
			charStream.ReInit(sqlText, 1, 1, LARGE_TOKEN_SIZE);
		}

		/* remember the string that we're parsing */
		SQLtext = statementSQLText;

		/* Parse the statement, and return the QueryTree */
		try
		{
		    return getParser().Statement(statementSQLText, paramDefaults);
		}
		catch (TokenMgrError e)
		{
			// Derby - 2103.
			// When the exception occurs cachedParser may live with
			// some flags set inappropriately that may cause Exception
			// in the subsequent compilation. This seems to be a javacc bug.
			// Issue Javacc-152 has been raised.
			// As a workaround, the cachedParser object is cleared to ensure
			// that the exception does not have any side effect.
			// TODO : Remove the following line if javacc-152 is fixed.
			cachedParser = null;
		    throw StandardException.newException(SQLState.LANG_LEXICAL_ERROR, e.getMessage());
		}
		catch (ParseException e)
		{
		    throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR, e.getMessage());
		}
	}

// GemStone changes BEGIN
        public int matchStatement(String statementSQLText,
            Object[] paramDefaults) throws StandardException {
      
          java.io.Reader sqlText = new java.io.StringReader(statementSQLText);
      
          /* Get a char stream if we don't have one already */
          if (matcherCharStream == null) {
            matcherCharStream = new UCode_MatcherCharStream(sqlText, 1, 1, LARGE_TOKEN_SIZE);
          }
          else {
            matcherCharStream.ReInit(sqlText, 1, 1, LARGE_TOKEN_SIZE);
          }
      
          /* remember the string that we're parsing */
          SQLtext = statementSQLText;
      
          /* Parse the statement, and return the QueryTree */
          try {
            return getMatcher().Statement(statementSQLText, paramDefaults);
          } catch (com.pivotal.gemfirexd.internal.engine.sql.compile.ParseException e) {
            throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR, e
                .getMessage());
          } catch (com.pivotal.gemfirexd.internal.engine.sql.compile.TokenMgrError e) {
            // Derby - 2103.
            // When the exception occurs cachedParser may live with
            // some flags set inappropriately that may cause Exception
            // in the subsequent compilation. This seems to be a javacc bug.
            // Issue Javacc-152 has been raised.
            // As a workaround, the cachedParser object is cleared to ensure
            // that the exception does not have any side effect.
            // TODO : Remove the following line if javacc-152 is fixed.
            cachedMatcher = null;
            SanityManager.DEBUG_PRINT("warning:"
                + GfxdConstants.TRACE_STATEMENT_MATCHING,
                "Exception while parsing generalized statement " + SQLtext
                + ": " + e);
            throw StandardException.newException(SQLState.LANG_LEXICAL_ERROR, e
                .getMessage());
            
          }
        }
// GemStone changes END

	/**
	 * Returns the current SQL text string that is being parsed.
	 *
	 * @return	Current SQL text string.
	 *
	 */
	public	String		getSQLtext()
	{	return	SQLtext; }
//      GemStone changes BEGIN
  @Override
  public void setGfxdSubactivationFlag(boolean flag)
  {
    this.gfxdSubactivationNeeded = flag;
    
  }
//GemStone changes END
}
