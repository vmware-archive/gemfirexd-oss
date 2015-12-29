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

import com.gemstone.gemfire.cache.CacheRuntimeException;

/**
 * RuntimeException propagated to invoking code to signal problem with remote
 * operations.
 *
 */
public class PartitionedRegionException extends CacheRuntimeException  {
private static final long serialVersionUID = 5113786059279106007L;

    /** Creates a new instance of ParititonedRegionException */
    public PartitionedRegionException() {
    }
    
    /** Creates a new instance of PartitionedRegionException 
     *@param msg 
     */
    public PartitionedRegionException(String msg) {
      super(msg);
    }    

    //////////////////////  Constructors  //////////////////////

    /**
     * Creates a new <code>PartitionedRegionException</code>.
     */
    public PartitionedRegionException(String message, Throwable cause) {
        super(message, cause);
    }
}
