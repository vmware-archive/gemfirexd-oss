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
package management.operations.events;

public interface FunctionOperationEvents {
  
  /* FunctionOperations Events */
  public void functionRegistered(String id);

  public void functionUnregistered(String id);

  public void functionExecuted(String id, Object result);

  public static String EVENT_FUNCT_REGISTER = "EVENT_FUNCT_REGISTER";
  public static String EVENT_FUNCT_UNREGISTER = "EVENT_FUNCT_UNREGISTER";
  public static String EVENT_FUNCT_EXEC = "EVENT_FUNCT_EXEC";

}
