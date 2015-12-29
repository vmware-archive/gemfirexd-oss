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
package com.gemstone.gemfire.internal.memcached;

/**
 * encapsulate ResponseOpCodes for binary reply messages.
 * 
 * @author Swapnil Bawaskar
 */
public enum ResponseStatus {

  NO_ERROR {
    @Override
    public short asShort() {
      return 0x0000;
    }
  },
  KEY_NOT_FOUND {
    @Override
    public short asShort() {
      return 0x0001;
    }
  },
  KEY_EXISTS {
    @Override
    public short asShort() {
      return 0x0002;
    }
  },
  ITEM_NOT_STORED {
    @Override
    public short asShort() {
      return 0x0005;
    }
  },
  NOT_SUPPORTED {
    @Override
    public short asShort() {
      return 0x0083;
    }
    
  };
  
  public abstract short asShort();
}
