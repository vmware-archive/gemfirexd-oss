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
package util;

import java.lang.reflect.*;
import util.*;

/** Class to coordinate execution of a method such that it only gets run
 *  once per VM even though it is called concurrently.
 *
 *  To used this, create an instance of this class giving a static method
 *  name and class. For every thread that tries to execute the given
 *  class and method name via a call to executeOnce, only one thread
 *  will actually execute it. After one thread has executed it, all other
 *  calls to executeOnce() is a noop. To execute the same method again in a
 *  "once per VM" manner, you must create a new instance of this class.
 */
public class MethodCoordinator {

private boolean completed = false;
private String className = null;
private String methodName = null;

/** Constructor to specify a class and method to execute
 */
public MethodCoordinator(String className, String methodName) {
   this.className = className;
   this.methodName = methodName;
}
    
/** Execute this instance's class/method one time only. All threads
 *  calling this method will only result in one of them actually
 *  executing the method. Others will just be a noop. To execute
 *  this method again later, a new instance of this class must be
 *  created.
 *
 *  @param receiver The object to receive the method; can be null
 *         for a static method.
 *  @param args The arguments used to invoke the method.
 *
 *  @return The return value of executing this instance's method.
 */
public synchronized Object executeOnce(Object receiver, Object[] args) {
   if (!completed) {
      Class aClass = null;
      try {
         aClass = Class.forName(className);
      } catch (ClassNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
      Method[] methodArr = aClass.getDeclaredMethods();
      Method aMethod = null;
      for (int i = 0; i < methodArr.length; i++) {
         aMethod = methodArr[i];
         if (aMethod.getName().equals(methodName)) {
            break;
         }
      }
      if (aMethod == null) {
         throw new TestException("Unable to find " + methodName + " in " + className);
      } 
      aMethod.setAccessible(true);
      try {
         Object returnObj = aMethod.invoke(receiver, args);
         completed = true;
         return returnObj; 
      } catch (InvocationTargetException e) {
         throw new TestException(TestHelper.getStackTrace(e.getTargetException()));
      } catch (IllegalAccessException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      } 
   }
   return null;
}

/** Return whether this instance's method has been executed or not.
 */
public boolean methodWasExecuted() {
   return completed;
}

}
