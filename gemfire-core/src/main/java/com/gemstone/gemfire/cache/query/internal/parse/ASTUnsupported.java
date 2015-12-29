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
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * AST class used for an AST node that cannot be compiled directly
 * because it is either data for another operation or is
 * a feature that is not yet supported by GemFire
 *
 * @author Eric Zoerner
 */
public class ASTUnsupported extends GemFireAST {
  private static final long serialVersionUID = -1192307218047393827L;
  
  public ASTUnsupported() {  
  }
  
  public ASTUnsupported(Token t) {
    super(t);
  }
  
  @Override
  public void compile(QCompiler compiler) {
    throw new UnsupportedOperationException(LocalizedStrings.ASTUnsupported_UNSUPPORTED_FEATURE_0.toLocalizedString(getText()));
  }
}
