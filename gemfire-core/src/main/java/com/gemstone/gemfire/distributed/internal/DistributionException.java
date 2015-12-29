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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.GemFireException;

/**
 * This exception is thrown when a problem occurs when accessing the
 * underlying distrubtion system (JGroups).  It most often wraps
 * another (checked) exception.
 */
public class DistributionException extends GemFireException {
private static final long serialVersionUID = 9039055444056269504L;

  /**
   * Creates a new <code>DistributionException</code> with the given
   * cause. 
   */
  public DistributionException(String message, Throwable cause) {
    super(message, cause);
  }

}
