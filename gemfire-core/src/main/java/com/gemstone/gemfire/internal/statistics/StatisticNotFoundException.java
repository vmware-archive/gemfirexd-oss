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
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.GemFireCheckedException;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class StatisticNotFoundException extends GemFireCheckedException {
  
  private static final long serialVersionUID = -6232790142851058203L;

  /**
   * Creates a new <code>StatisticNotFoundException</code> with no detailed message.
   */
  public StatisticNotFoundException() {
    super();
  }

  /**
   * Creates a new <code>StatisticNotFoundException</code> with the given detail
   * message.
   */
  public StatisticNotFoundException(String message) {
    super(message);
  }

  /**
   * Creates a new <code>StatisticNotFoundException</code> with the given detail
   * message and cause.
   */
  public StatisticNotFoundException(String message, Throwable cause) {
    super(message);
    this.initCause(cause);
  }
  
  /**
   * Creates a new <code>StatisticNotFoundException</code> with the given cause and
   * no detail message
   */
  public StatisticNotFoundException(Throwable cause) {
    super();
    this.initCause(cause);
  }
  
}
