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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.LocalField;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedClass;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CodeGeneration;
import com.pivotal.gemfirexd.internal.iapi.sql.compile.CompilerContext;
import com.pivotal.gemfirexd.internal.iapi.util.ByteArray;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;


/**
 *
 * 
 * @author soubhikc
 *
 */
public class GemFireExpressionClassBuilder extends ExpressionClassBuilder {

  public GemFireExpressionClassBuilder(String superClass, String className,
      CompilerContext cc) throws StandardException {
    super(superClass, className, cc);
  }
  
  /**
   * Get the name of the package that the generated class will live in.
   *
   *      @return name of package that the generated class will live in.
   */
  @Override
  protected String  getPackageName() {
     return  CodeGeneration.GENERATED_PACKAGE_PREFIX; 
  }

  /**
   * Get the number of ExecRows that must be allocated
   *
   *      @return number of ExecRows that must be allocated
   *
   *      @exception StandardException thrown on failure
   */
  @Override
  protected int getRowCount() throws StandardException {
    return myCompCtx.getNumResultSets();
  }

  /**
   * Sets the number of subqueries under this expression
   *
   *
   *      @exception StandardException thrown on failure
   */
  @Override
  protected void setNumSubqueries() throws StandardException {
    SanityManager.THROWASSERT("method not expected to get called. "
        + "time to inherit from ActivationClassBuilder");
  }
  
  @Override
  protected String getBaseClassName() {
    return ClassName.BaseActivation;
  }

  /**
   * The first time a current datetime is needed, create the class level support
   * for it. The first half of the logic is in our parent class.
   */
  @Override
  protected LocalField getCurrentSetup() {
    if (cdtField != null) {
      return cdtField;
    }

    LocalField lf = super.getCurrentSetup();

    // 3) the execute method gets a statement (prior to the return)
    // to tell cdt to restart:
    // cdt.forget();

    executeMethod.getField(lf);
    executeMethod.callMethod(VMOpcode.INVOKEVIRTUAL, (String)null, "forget",
        "void", 0);

    return lf;
  }

  public MethodBuilder getNewExpressionMethodBuilder() {
    return (this.executeMethod = newExprFun());
  }
  
  public GeneratedClass getGeneratedClass() 
    throws StandardException {
    return getGeneratedClass(null);
  }

  public ByteArray getClassBytecode() 
    throws StandardException {
    return cb.getClassBytecode();
  }
  
  public void finishConstruct() 
   throws StandardException {

    addNewArrayOfRows(1);
    constructor.methodReturn();
    constructor.complete();
  }
}
