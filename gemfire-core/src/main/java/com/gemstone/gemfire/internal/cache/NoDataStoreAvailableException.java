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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.GemFireException;

/**
 * This Exception is used by GemFireXD to identify situation when
 * UpdateOperation or DestroyOperation done on a replicated region is not
 * distributed due to absence of data stores in the system
 * 
 * @author Asif
 * 
 */
public class NoDataStoreAvailableException extends GemFireException {

  private static final long serialVersionUID = -6471045959318795871L;

  public NoDataStoreAvailableException(String message) {
    super(message);
  }

}
