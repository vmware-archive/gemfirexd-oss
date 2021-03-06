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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;

/**
 * Delegates object comparisons to one or more embedded comparators.
 *  
 * @author bakera
 */
public interface DelegatingSerializedComparator extends SerializedComparator {
  /**
   * Injects the embedded comparators.
   * @param comparators the comparators for delegation
   */
  void setComparators(SerializedComparator[] comparators);
  
  /**
   * Returns the embedded comparators.
   * @return the comparators
   */
  SerializedComparator[] getComparators();
}
