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

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.JDBC30Translation;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueFactory;
import com.pivotal.gemfirexd.internal.impl.sql.GenericParameterValueSet;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

public class ProxyParameterValueSet implements ParameterValueSet {
  
  private Activation activation;
  private GenericParameterValueSet pvs;
  volatile boolean  ready;
  private ProcedureResultProcessor prp;
  
  ProxyParameterValueSet(Activation activation,
      GenericParameterValueSet pvs,
      ProcedureResultProcessor prp) {
    this.activation=activation;
    this.pvs=pvs;    
    this.prp=prp;
  }

  public int allAreSet() {
    return this.pvs.allAreSet();

  }

  public boolean checkNoDeclaredOutputParameters() { 
    return this.pvs.checkNoDeclaredOutputParameters();
  }

  public void clearParameters() {   
    this.pvs.clearParameters();

  }

  public ParameterValueSet getClone() {
    checkReady();
    return this.pvs.getClone();
  }

  public DataValueDescriptor getParameter(int position)
      throws StandardException {
    checkReady();
    return this.pvs.getParameter(position);
  }

  public int getParameterCount() {   
    return this.pvs.getParameterCount();
  }

  public DataValueDescriptor getParameterForGet(int position)
      throws StandardException {
    checkReady();
    return this.pvs.getParameterForGet(position);
  }

  public DataValueDescriptor getParameterForSet(int position)
      throws StandardException {
    checkReady();
    return this.pvs.getParameterForSet(position);
  }

  public short getParameterMode(int parameterIndex) {    
    return this.pvs.getParameterMode(parameterIndex);
  }

  public int getPrecision(int parameterIndex) {
    checkReady();
    return this.pvs.getPrecision(parameterIndex);
  }

  public DataValueDescriptor getReturnValueForSet() throws StandardException {
    checkReady();
    return this.pvs.getReturnValueForSet();
  }

  public int getScale(int parameterIndex) {
    checkReady();
    return this.pvs.getScale(parameterIndex);
  }

  public boolean hasReturnOutputParameter() {    
    return this.pvs.hasReturnOutputParameter();
  }

  public void initialize(DataTypeDescriptor[] types) throws StandardException {
    this.pvs.initialize(types);

  }

  public void registerOutParameter(int parameterIndex, int sqlType, int scale)
      throws StandardException {
    
    this.pvs.registerOutParameter(parameterIndex, sqlType, scale);

  }

  public void setParameterAsObject(int parameterIndex, Object value)
      throws StandardException {
    
    this.pvs.setParameterAsObject(parameterIndex, value);

  }

  public void setParameterMode(int position, int mode) {
    this.pvs.setParameterMode(position, mode);

  }

  public void transferDataValues(ParameterValueSet pvstarget)
      throws StandardException {
    this.pvs.transferDataValues(pvstarget);

  }

  public void validate() throws StandardException {
    this.pvs.validate();

  }
  /**
   * Assume only one thread accesses the parameters . 
   * 
   *     
   */
  
  private void checkReady() {
    if(this.ready) {
      return;
    }
    while(true) {
    try {

        Object[] parameters=this.prp.getOutParameters();
        if(parameters==null) {
           this.ready=true;
           return;
        }
        DataValueFactory dvf=this.activation.getDataValueFactory();
        int index=0;        
        int numParameters=this.pvs.getParameterCount();
        for(int i=1; i<=numParameters; i++) {
           int mode=pvs.getParameterMode(i);
           if(mode==JDBC30Translation.PARAMETER_MODE_IN_OUT ||
               mode==JDBC30Translation.PARAMETER_MODE_OUT) {
             
              DataValueDescriptor param=pvs.getParameter(i-1);
              assert param!=null:"the dvd should not be null!";
              Object val=parameters[index];
              if(val==null) {
                  param.setToNull();                 
              }
              else 
              {
                 DataValueDescriptor dvd=dvf.getDataValue(val);
                 param.setValue(dvd);
              }   
              ++index;
                           
           }
          
        }
        this.ready=true;
        return;
        
        

     }
      catch (InterruptedException e) {
        // @yjing to do make sure what needs to be done when this kind of
        // exception happens
        // now only ignore it.
      }
      catch (StandardException e) {
        throw new AssertionError("Error happens in the checkReady() in the ProxyParameterValueSet!");
      }
    }
   
  }
  @Override
  public boolean isListOfConstants() {
    return pvs.isListOfConstants();
  }
  @Override
  public boolean canReleaseOnClose() {
    return pvs.canReleaseOnClose();
  }
}
