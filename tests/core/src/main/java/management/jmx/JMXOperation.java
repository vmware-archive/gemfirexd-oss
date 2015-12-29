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
package management.jmx;

import javax.management.ObjectName;

public class JMXOperation implements JMXEvent {

  private static final long serialVersionUID = 1866082563059579523L;
  private String source;
  private String operationName;
  private Object result;
  private Throwable error;
  private String message;
  private ObjectName name;

  public JMXOperation(ObjectName name, String operation, Object result, Throwable error, String message) {
    this.name = name;
    this.operationName = operation;
    this.result = result;
    this.error = error;
    this.message = message;
  }

  @Override
  public boolean isNotification() {
    return false;
  }

  @Override
  public boolean isOpertion() {
    return true;
  }

  public static long getSerialversionuid() {
    return serialVersionUID;
  }

  public String getSource() {
    return source;
  }

  public String getOperationName() {
    return operationName;
  }

  public Object getResult() {
    return result;
  }

  public Throwable getError() {
    return error;
  }

  public boolean isError() {
    return (error != null) || (message != null && message.contains("Error"));
  }

  @Override
  public ObjectName getObjectName() {
    return name;
  }

}
