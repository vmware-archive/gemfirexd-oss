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
package com.pivotal.gemfirexd.internal.engine.procedure.coordinate;

import com.pivotal.gemfirexd.internal.catalog.AliasInfo;
import com.pivotal.gemfirexd.internal.catalog.types.SynonymAliasInfo;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.ClassName;
import com.pivotal.gemfirexd.internal.iapi.services.classfile.VMOpcode;
import com.pivotal.gemfirexd.internal.iapi.services.compiler.MethodBuilder;
import com.pivotal.gemfirexd.internal.iapi.services.loader.ClassInspector;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.AliasDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ExpressionClassBuilder;
import com.pivotal.gemfirexd.internal.impl.sql.compile.QueryTreeNode;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

public class ProcedureProcessorNode extends QueryTreeNode {
  private String className;

  private String aliasName;

  private boolean defaultProcessor = false;

  public ProcedureProcessorNode() {

  }

  @Override
  public void init(Object arg1, Object arg2) throws StandardException {
    this.className = (String)arg1;
    this.aliasName = (String)arg2;
  }

  public void bindExpression(DataDictionary dd) throws StandardException {    
    // check the result processor class exist, access, and implement the
    // required interface.
    this.className = getClassName(dd);
    if (this.className != null) {
      ClassInspector ci = getClassFactory().getClassInspector();
      try {
        if (!ci.accessible(this.className)) {
          throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST2, this.className);
        }
      }
      catch (ClassNotFoundException e) {
        throw StandardException.newException(SQLState.LANG_TYPE_DOESNT_EXIST2, this.className);
      }
      if (!ci.assignableTo(this.className, ClassName.ProcedureResultProcessor)) {
        throw StandardException.newException(
            SQLState.DAP_RESULT_PROCESSOR_INTERFACE_MISSING, this.className,
            ClassName.ProcedureResultProcessor);
      }
    }
    else {
      this.defaultProcessor = true;
    }
  }

  private String getClassName(DataDictionary dd) throws StandardException {
    if (this.className != null) {
      return this.className;
    }
    if (this.aliasName != null) {
      SchemaDescriptor sd = Misc.getLanguageConnectionContext().getDefaultSchema();
      AliasDescriptor ad = dd.getAliasDescriptor(sd.getUUID().toString(), this.aliasName,
          AliasInfo.ALIAS_TYPE_RESULT_PROCESSOR_AS_CHAR);
      if (ad == null) {
        // assume that class name was provided
        this.className = this.aliasName;
        this.aliasName = null;
        return this.className;
      }
      AliasInfo ainfo = ad.getAliasInfo();
      assert ainfo != null;
      assert ainfo instanceof SynonymAliasInfo : "alias info type is: " + ainfo.getClass();
      this.className = ((SynonymAliasInfo)ainfo).getSynonymTable();
    }
    return this.className;
  }

  public void generateExpression(ExpressionClassBuilder acb, MethodBuilder mb)
      throws StandardException {
    if (this.defaultProcessor) {
      acb.pushThisAsActivation(mb);
      mb.callMethod(VMOpcode.INVOKEINTERFACE, null, "getExecutionFactory",
          ClassName.ExecutionFactory, 0);
      mb.callMethod(VMOpcode.INVOKEINTERFACE, null,
          "getDefaultProcedureResultProcessor",
          ClassName.ProcedureResultProcessor, 0);

    }
    else {
      mb.pushNewStart(this.className);
      mb.pushNewComplete(0);
      mb.upCast(ClassName.ProcedureResultProcessor);

    }
  }
}
