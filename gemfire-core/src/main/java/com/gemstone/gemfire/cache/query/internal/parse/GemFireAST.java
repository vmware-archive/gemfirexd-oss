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
//import antlr.collections.*;
//import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.QCompiler;

/**
 *
 * @author Eric Zoerner
 */
public class GemFireAST extends CommonAST {
  private static final long serialVersionUID = 779964802274305208L;
  
  public GemFireAST() {
    super();
  }
  
  public GemFireAST(Token tok) {
    super(tok);
  }
  
  @Override
  public String getText() {
    String txt = super.getText();
    if (txt == null) {
      return "[no text]";
    }
    return txt;
  }
  
  public void compile(QCompiler compiler)  {
    childrenCompile(compiler);
  }
  
  public void childrenCompile(QCompiler compiler) {
    GemFireAST child = (GemFireAST)getFirstChild();
    while (child != null) {
      child.compile(compiler);
      child = (GemFireAST)child.getNextSibling();
    }
  }
  
}
