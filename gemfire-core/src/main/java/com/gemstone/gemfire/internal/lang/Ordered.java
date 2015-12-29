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
 * The Ordered interface defines a contract for implementing classes who's instances must participate in some type
 * of ordered data structure, such as an array or List, or exist in a context where order relative to other
 * peer instances matter.
 * <p/>
 * @author John Blum
 * @since 6.8
 */
public interface Ordered {

  /**
   * Gets the order of this instance relative to it's peers.
   * <p/>
   * @return an integer value indicating the order of this instance relative to it's peers.
   */
  public int getIndex();

  /**
   * Sets the order of this instance relative to it's peers.
   * <p/>
   * @param index an integer value specifying the the order of this instance relative to it's peers.
   */
  public void setIndex(int index);

}
