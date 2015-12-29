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

package com.pivotal.gemfirexd.internal.engine.ddl;

/**
 * This class just holds the initial size of the table passed at the time of
 * table creation.
 * 
 * @author kneeraj
 * @since 6.0
 */
public class InitSizeTableAttribute {

  private final int initSize;

  public InitSizeTableAttribute(int initsize) {
    this.initSize = initsize;
  }

  public int getInitialSize() {
    return this.initSize;
  }
}
