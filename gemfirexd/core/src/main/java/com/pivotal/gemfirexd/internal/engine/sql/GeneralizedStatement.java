/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.engine.sql;

import java.util.List;

import com.gemstone.gnu.trove.THashMap;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.compile.Token;
import com.pivotal.gemfirexd.internal.engine.sql.execute.ConstantValueSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;
import com.pivotal.gemfirexd.internal.impl.sql.conn.GenericLanguageConnectionContext;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

/**
 * Generalized Statement against which GenericPreparedStatement is cached
 * when statement.execute(...) gets called. This is primarily to save 
 * unnecessary temporary objects creation during lookup of the generalized
 * version of the statement.
 *
 * e.g. select * from tab where id = 'xxxx' and where id = 'yyy' will be stored
 * against a single plan select * from tab where id = CHAR.
 * 
 * @author soubhikc
 *
 */
public final class GeneralizedStatement extends GenericStatement {

  

  public static final String CONSTANT_PLACEHOLDER = "<?>";
  
  public GeneralizedStatement(SchemaDescriptor compilationSchema,
      String statementText,short execFlags, THashMap ncjMetaData) {
    
    //let super compute the hashcode.
    super(compilationSchema, statementText, execFlags, ncjMetaData);
    
    
  }

  //caller should ensure atomicity between Generalized statements compiled artifacts.
  public void setPreparedStatement(GenericPreparedStatement ps ) {
    preparedStmt = ps;
  }
 
  public void setSource(String sqlText, boolean createQueryInfo, SchemaDescriptor compilationSchema, CompilerContext cc) {
        
   
    //convert into generalized strings
    this.statementText = sqlText ;//sgeneralizedStatement(sqlText,this.constantTokenList);
    if( createQueryInfo) {
      this.execFlags = GemFireXDUtils.set(execFlags, CREATE_QUERY_INFO);
    }else {
      this.execFlags = GemFireXDUtils.clear(execFlags, CREATE_QUERY_INFO);
    }    
    this.compilationSchema = compilationSchema;
    this.hash = getHashCode();
  }

  @Override
  public String getQueryStringForParse(LanguageConnectionContext lcc) {
    return recreateUserQuery(this.statementText,
        (ConstantValueSet)lcc.getConstantValueSet(null));
  }

  public void reset() {
    this.statementText = null;
    this.execFlags = GemFireXDUtils.clear(execFlags, CREATE_QUERY_INFO);    
    this.compilationSchema = null;
    this.hash = -1;
  }
 
  /**
   * This will happen occasionally mainly on data nodes,
   * when generalized statement will trigger query compilation 
   * (reprepare etc). During that time we don't want to
   * keep incoming ConstantValueSet from message.
   * 
   * @param lcc
   * @return original query string submitted by user.
   * @throws StandardException 
   */
  public String restoreOriginalString(final GenericLanguageConnectionContext lcc) throws StandardException
  {
    ConstantValueSet constants = (ConstantValueSet)lcc.getConstantValueSet(null);
    
    if (SanityManager.DEBUG) {
      if (constants == null) {
        SanityManager
            .THROWASSERT("Cannot restore original string as no constantValueSet is available yet");
      }
    }
    
    return recreateUserQuery(statementText, constants);
  }
  
  /**
   * This method is costly in terms of Eden space memory. Need to come back and review whether to
   * store original string per activation or other way to recreate this string.
   * @param sql
   * @param constants
   * @return
   */
  public static String recreateUserQuery(String sql, final ConstantValueSet constants) {
    
    int index = sql.indexOf(CONSTANT_PLACEHOLDER, 0);
    int constantIdx = 0;
    int len =0;
    if(constants != null) {
      len = constants.getParameterCount();
    }
    for (; index != -1 && index < sql.length() && constantIdx < len; constantIdx++) {
      sql = sql.substring(0, index) + constants.getConstantImage(constantIdx)
          + sql.substring(index + CONSTANT_PLACEHOLDER.length());
      index = sql.indexOf(CONSTANT_PLACEHOLDER, index + 1);
    }

    if (GemFireXDUtils.TraceStatementMatching) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
          "returning restored user query " + sql);
    }
    return sql;
  }



  @Override
  public boolean equals(Object other) {

    if (! (other instanceof GeneralizedStatement) ) {
      return false;
    }

    final GeneralizedStatement os = (GeneralizedStatement)other;
    final GeneralizedStatement _this = this;

    return _this.isEquivalent(os)
        && _this.isForReadOnly() == os.isForReadOnly()
        && _this.compilationSchema.equals(os.compilationSchema) &&
        // GemStone changes BEGIN
        (_this.createQueryInfo() == os.createQueryInfo()) &&
        // GemStone changes END
        (_this.prepareIsolationLevel == os.prepareIsolationLevel);
  }
  
  /*
   * few things to know before changing anything over here.
   * 
   * a. while reparsing, we temporarily switch to original user
   * query & store generic query in a volatile field. 
   * 
   *    cannot synchronize parsing & compare as cache lookup will
   *    happen during parsing in prepMinion.
   *    
   * b. following four combinations 
   *    (i) generic string, user query
   *    (ii) user query , generic string
   *    (iii) user query, user query.
   *    (iv) generic query, generic query.
   *    
   * we handle as:
   *    (i) & (ii) is gone past the token list nullability check.
   *    
   *    (iii) is not possible as two Generalized statement
   *    comparisons must have one of them as user query (setSource) and
   *    therefore sqlmatcher must have put constantTokens in one of the two.
   *    
   *    Two user queries will never get compared as only generalized query 
   *    string will be inserted into the Statement Cache.
   *    
   *    (iv) simple equality check. this can only happen when two connections 
   *    are trying to introduce generic statement to the statement cache.
   */
  private boolean isEquivalent(final GeneralizedStatement other) {
    if ( this == other)
      return true;

    /* don't re-order or insert anything between next 4 lines.
     * if you need to alter the memory barrier, make sure
     * restoreOriginalString() is honored for concurrency.
     */
    //final String origQuery = this.originalGenericQuery;
    //final String ttxt = this.statementText;
    
    //final String oorigQuery = other.originalGenericQuery;
    //final String otxt = other.statementText;
    // end of no change zone.

   // final ArrayList<Token> thistokens =  this.constantTokenList;
    //final ArrayList<Token> othertokens = other.constantTokenList;
    
    final String thisstmt  = this.statementText;// origQuery == null ? ttxt : origQuery;
    final String otherstmt = other.statementText;//oorigQuery == null ? otxt : oorigQuery;
    
    final String strmsg;
    if (GemFireXDUtils.TraceStatementMatching) {
      strmsg = "lhs=[" + thisstmt + "] rhs=[" + otherstmt + "]  this=" + this
          + "  other=" + other;
    }
    else {
      strmsg = null;
    }
    
    if (SanityManager.ASSERT) {
      if (thisstmt == null || otherstmt == null) {
        SanityManager.THROWASSERT(strmsg == null ? "lhs=" + this + " rhs="
            + other : strmsg);
      }
    }
    
    //if( thistokens == null && othertokens == null) {
  
      final boolean isequal = thisstmt.equals(otherstmt);
     
      
      return isequal;
   
  }

  private final int getHashCode() {
    // final ArrayList<Token> thistokens = this.constantTokenList;
    final String stmt = statementText;

    assert this.isPreparedStatement() == false
        && this.isOptimizedStatement() == true;// && thistokens != null;

    int h = super.getHashCode(0, stmt.length(), 0);
    this.stmtHash = h;
    h = ResolverUtils.addIntToHash(createQueryInfo() ? 1231 : 1237, h);
    return ResolverUtils.addIntToHash(isPreparedStatement()
        || !isOptimizedStatement() ? 19531 : 20161, h);
    // resulthash = getGenericStatementHash(stmt, thistokens, resulthash);

    // return resulthash;
  }

  /*
  @Override
  protected int getUniqueIdFromStatementText(LanguageConnectionContext lcc) {
    return recreateUserQuery(this.statementText,
        (ConstantValueSet)lcc.getConstantValueSet(null)).hashCode();
  }

  //Can be removed.
  private int getGenericStatementHash(final String statementText, final ArrayList<Token> thistokens, int resulthash) {
    int srcPos = 0;
    int strlen = statementText.length();
    
    int count = 0;
    for(com.pivotal.gemfirexd.internal.engine.sql.compile.Token v : thistokens) {
      
      final int beginC = v.beginOffset ;
      ++count;
      assert beginC >= srcPos;
      final int endC = v.endOffset;

      resulthash = super.getHashCode(srcPos, beginC, resulthash);

      if (GemFireXDUtils.TraceStatementMatching) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
            statementText + " skipping constant: " + v);
      }

      srcPos = endC + 1;
      resulthash = ResolverUtils.addIntToHash(CONSTANT_PLACEHOLDER.hashCode(),
          resulthash);
    }

    if(srcPos < strlen) {
      resulthash = super.getHashCode(srcPos, strlen, resulthash);
    }
    
    return resulthash;
  }
  */

  @Override
  public String toString() {

    final String st;
    final boolean isCompiling;
    synchronized (this) {
      
        st = this.statementText;
        isCompiling = true;
      
    }

    return GemFireXDUtils.addressOf(this)
        + (!isCompiling ? (' ' + st) : " compilingQuery=" + this.statementText
            + " genericQuery=" + st);
        //+ (c != null && c.size() > 0 ? " with constants " + c :"");
  }
  
public static String generalizedStatement(String statementText, List<Token> constantTokenList) {
    
    assert constantTokenList != null;

    final StringBuilder generalizedStmtStr = new StringBuilder(statementText.length());

    // if there are constants, need to generalize the statement.
    if (constantTokenList.size() != 0) {
      // now that we know there are constants ... lets skip them and
      // build the generic version of the statement
      int startPos = 0;
      for (final Token v : constantTokenList) {
        
        final int beginC = v.beginOffset;

        final String str = statementText.substring(startPos, beginC);
        
        startPos = v.endOffset;

        generalizedStmtStr.append(str).append(GeneralizedStatement.CONSTANT_PLACEHOLDER); //v.valueImage.getTypeName());
      }

      final int cLen = statementText.length();
      if (startPos < cLen) {
        final String str = statementText.substring(startPos, cLen);
        generalizedStmtStr.append(str);
      }
    }

    if (GemFireXDUtils.TraceStatementMatching) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATEMENT_MATCHING,
          "returning generalizedStmt " + generalizedStmtStr.toString());
    }
    return generalizedStmtStr.toString();
  }
}
