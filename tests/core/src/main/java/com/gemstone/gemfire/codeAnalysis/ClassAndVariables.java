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
package com.gemstone.gemfire.codeAnalysis;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;
import com.gemstone.gemfire.codeAnalysis.decode.CompiledField;



public class ClassAndVariables implements Comparable {
  public CompiledClass dclass;
  public boolean hasSerialVersionUID = false;
  public long serialVersionUID;
  public Map<String,CompiledField> variables = new HashMap<String,CompiledField>();
  
  public ClassAndVariables(CompiledClass parsedClass) {
    this.dclass = parsedClass;
    
    String name = dclass.fullyQualifiedName().replace('/', '.');
    try {
      Class realClass = Class.forName(name);
      Field field = realClass.getDeclaredField("serialVersionUID");
      field.setAccessible(true);
      serialVersionUID = field.getLong(null);
      hasSerialVersionUID = true;
    } catch (NoSuchFieldException e) {
      //No serialVersionUID defined
      
    } catch (Throwable e) {  
      System.out.println("Unable to load" + name + ":" + e);
    }
    
  }
  
  public int compareTo(Object other) {
    if ( !(other instanceof ClassAndVariables) ) {
      return -1;
    }
    return dclass.compareTo(((ClassAndVariables)other).dclass);
  }
  
  
  @Override public String toString() {
    return ClassAndVariableDetails.convertForStoring(this);
  }

}