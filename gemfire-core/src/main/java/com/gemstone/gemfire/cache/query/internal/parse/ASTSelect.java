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


package com.gemstone.gemfire.cache.query.internal.parse;

import antlr.*;
import antlr.collections.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

public class ASTSelect extends GemFireAST {
  private static final long serialVersionUID = 1389351692304773456L;
  
  
  public ASTSelect() { }
  
  
  public ASTSelect(Token t) {
    super(t);
  }
  
  
  @Override
  public void compile(QCompiler compiler) {
  	AST child = getFirstChild();
    
    // check for DISTINCT or ALL token
    // if DISTINCT, push "DISTINCT" onto stack, otherwise push null
    // if found, then advance to next sibling, otherwise this child
    // must be projection
    if (child.getType() == OQLLexerTokenTypes.LITERAL_distinct) {
      compiler.push("DISTINCT"); // anything non-null works here for distinct
      child = child.getNextSibling();
    }
    else if (child.getType() == OQLLexerTokenTypes.LITERAL_all) {
      compiler.pushNull();
      child = child.getNextSibling();
    } 
    else {
      compiler.pushNull(); // let child be next in line
    }
    
    //Count(*) expression
    if (child.getType() == OQLLexerTokenTypes.LITERAL_count) {
      ((ASTCount)child).compile(compiler);
      compiler.pushNull(); //For No projectionAttributes
    } else {
      compiler.pushNull();
      // projectionAttributes
      if (child.getType() == OQLLexerTokenTypes.TOK_STAR) {
        compiler.pushNull();
      }
      else {
        // child is ASTCombination; compile it
        ((ASTCombination)child).compile(compiler);
      }
    }

    // fromClause
    child = child.getNextSibling(); 
    ((GemFireAST)child).compile(compiler);
   
      
	/*If WHERE clause ,order by clause as well as Limit clause is missing, then push 3 null as a placeholder */ 
    if (child.getNextSibling() == null) {
      compiler.pushNull();
      compiler.pushNull();
      //Asif: This placeholder is for limit 
      compiler.pushNull();
    }
    else { 
       child = child.getNextSibling();      
       int clauseType = child.getType();
       if( clauseType != OQLLexerTokenTypes.LITERAL_order && clauseType != OQLLexerTokenTypes.LIMIT ) {
         //  where is present , order by & limit may present |  may !present
         ((GemFireAST)child).compile(compiler);
         child = child.getNextSibling();
         if(child != null) {
           clauseType = child.getType();
         }
         
       }else {
         //Where clause is null
         compiler.pushNull();         
       }
       if(clauseType == OQLLexerTokenTypes.LITERAL_order) {
         ((GemFireAST)child).compile(compiler);
         child = child.getNextSibling();
         if(child != null) {
           clauseType = child.getType();
         } 
       }else {
         //Order by clause is null
         compiler.pushNull();
       }
       
       if(clauseType == OQLLexerTokenTypes.LIMIT) {
         ((GemFireAST)child).compile(compiler);         
       }else {
         //Limit clause is null
         compiler.pushNull();
       }
    }    
    compiler.select();
  }
  
  
}
