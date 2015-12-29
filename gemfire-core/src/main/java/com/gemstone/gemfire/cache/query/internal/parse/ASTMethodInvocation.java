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

import com.gemstone.gemfire.cache.query.internal.QCompiler;
import com.gemstone.gemfire.i18n.LogWriterI18n;

public class ASTMethodInvocation extends GemFireAST {
  private static final long serialVersionUID = -3158542132262327470L;
  private boolean implicitReceiver;
    
  
  public ASTMethodInvocation() { }    
  
  public ASTMethodInvocation(Token t) {
    super(t);
  }
  
  public void setImplicitReceiver(boolean implicitReceiver) {
    this.implicitReceiver = implicitReceiver;
  }
  
  @Override
  public int getType() {
    return OQLLexerTokenTypes.METHOD_INV;
  }
  
  
  
  @Override
  public void compile(QCompiler compiler) {
    LogWriterI18n logger = compiler.getLogger();
    if (logger.finerEnabled()) {
      logger.finer("ASTMethodInvocation.compile: implicitReceiver=" + implicitReceiver);
    } 
    if (this.implicitReceiver) {
      compiler.pushNull(); // placeholder for receiver
    }
    
    // push the methodName and argList on the stack
    super.compile(compiler);
    
    compiler.methodInvocation();
  }
  
  
}
