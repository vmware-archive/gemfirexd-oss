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
/**
 * 
 */
package com.gemstone.gemfire.distributed.internal;

/**
 * thrown by {@linkplain ReliableReplyProcessor21} when a message has not been delivered
 * to at least one member in the original recipient list.
 * @author sbawaska
 */
public class ReliableReplyException extends ReplyException {
  private static final long serialVersionUID = 472566058783450438L;

  public ReliableReplyException(String message) {
    super(message);
  }
  
  public ReliableReplyException(String message, Throwable cause) {
    super(message, cause);
  }
}
