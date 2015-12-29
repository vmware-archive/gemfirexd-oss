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

package com.gemstone.gemfire.admin;

//import com.gemstone.gemfire.GemFireException;

/**
 * Thrown when an administration operation that accesses information
 * in a remote system member is cancelled.  The cancelation may occur
 * because the system member has left the distributed system.
 *
 * @since 3.5
 * @deprecated as of 7.0 use the {@link com.gemstone.gemfire.management} package instead
 */
public class OperationCancelledException extends RuntimeAdminException {
   private static final long serialVersionUID = 5474068770227602546L;
    
    public OperationCancelledException() {
      super();
    }
    
    public OperationCancelledException( String message ) {
        super( message );
    }
    
    public OperationCancelledException( Throwable cause ){
      super(cause);
    }
    
    public OperationCancelledException( String message, Throwable cause ) {
      super(message, cause);
    }
}
