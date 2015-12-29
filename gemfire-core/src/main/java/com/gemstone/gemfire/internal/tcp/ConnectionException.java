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
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.GemFireException;

/**
    @author Bruce Schuchardt
    @since 3.0
   
 */
public class ConnectionException extends GemFireException
{
  private static final long serialVersionUID = -1977443644277412122L;

  public ConnectionException(String message) {
     super(message);
  }

  public ConnectionException(String message, Throwable cause) {
     super(message, cause);
  }
  
}
