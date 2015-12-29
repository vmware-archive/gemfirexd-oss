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
package com.gemstone.gemfire.management;

/**
 * Indicates that a request to start a management service
 * failed because it was already running.
 * 
 * @author darrel
 * @since 7.0
 * 
 */
public class AlreadyRunningException extends ManagementException {

  private static final long serialVersionUID = 8947734854770335071L;

  public AlreadyRunningException() {
  }

  public AlreadyRunningException(String message) {
    super(message);
  }

  public AlreadyRunningException(String message, Throwable cause) {
    super(message, cause);
  }

  public AlreadyRunningException(Throwable cause) {
    super(cause);
  }

}
