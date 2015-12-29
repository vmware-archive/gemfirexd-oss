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
package com.gemstone.gemfire.internal.lang;

/**
 * The Initializer class is a utility class to identify Initable objects and initialize them by calling their
 * init method.
 * <p/>
 * @author John Blum
 * @see com.gemstone.gemfire.internal.lang.Initable
 * @since 7.5
 */
public class Initializer {

  /**
   * Initializes the specified Object by calling it's init method if and only if the Object implements the
   * Initable interface.
   * <p/>
   * @param initableObj the Object targeted to be initialized.
   * @return true if the target Object was initialized using an init method; false otherwise.
   * @see com.gemstone.gemfire.internal.lang.Initable#init()
   */
  public static boolean init(final Object initableObj) {
    if (initableObj instanceof Initable) {
      ((Initable) initableObj).init();
      return true;
    }

    return false;
  }

}
