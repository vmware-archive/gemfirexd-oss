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
package com.pivotal.gemfirexd.internal.engine.sql.compile;

import com.pivotal.gemfirexd.internal.catalog.SystemProcedures;
import com.pivotal.gemfirexd.internal.engine.diag.DiagProcedures;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.Visitable;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.VisitorAdaptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.compile.StaticMethodCallNode;

/**
 * To verify whether we have only staticMethodCall functions in JavaToSQLValueNode.
 * 
 * @author soubhikc
 *
 */
public final class VerifySysfunctionExpressionVisitor extends VisitorAdaptor {

  private boolean isStaticMethodFound = false;
  
  private static final String[] procedureClasses = new String[] {
    SystemProcedures.class.getName(),
    DiagProcedures.class.getName()
  };
  
  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return isStaticMethodFound;
  }

  @Override
  public boolean stopTraversal() {
    return isStaticMethodFound;
  }

  @Override
  public Visitable visit(Visitable node) throws StandardException {
    
    if(node instanceof StaticMethodCallNode) {
      //only for sys functions defined in these classes.
      StaticMethodCallNode n = (StaticMethodCallNode) node;
      if( n.javaClassName != null) {
        for(final String pc : procedureClasses) {
          if(pc.equals(n.javaClassName)) {
            isStaticMethodFound = true;
            break;
          }
        }
      }
    }
    
    return node;
  }

  public boolean hasSysFun() {
    return isStaticMethodFound;
  }
  
}
