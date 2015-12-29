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
package management.operations.ops.jmx;

import java.io.Serializable;

import javax.management.ObjectName;

import management.operations.ops.JMXOperations;


public interface TestMBean extends Serializable{
  
  String getType();
  
  String[] getAttributes();
  
  Object[][] getOperations();
  
  /**
   * This method will return an method filled with
   * arguments and correct signature. Its job of corresponding
   * TestMBean to populate test-relevant data
   * 
   * Return objects contains following in the order given below
   * 
   * 0. Name
   * 1. Return Type
   * 2. Signature of Operations
   * 3. Expected Return value if any. Whenever this values is not-null
   *      client code is expected to add JMX Expectation against execution
   *      of this operation.
   * 
   * @param op
   
  Object[] getOperation(ObjectName name,String op);*/
  
  String getTemplateObjectName();
  
  String[] getTests();
  
  void executeTest(JMXOperations ops, ObjectName name);

  void doValidation(JMXOperations ops);

}
