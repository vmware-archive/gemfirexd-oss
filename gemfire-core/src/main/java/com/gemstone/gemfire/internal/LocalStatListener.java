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
package com.gemstone.gemfire.internal;

/**
 * Implement this interface to receive call back when a stat value has changed.
 * The listener has to be registered with statSampler. This can be done in the
 * following manner:
 * <code>
 * InternalDistributedSystem internalSystem = (InternalDistributedSystem)cache
 *                                                      .getDistributedSystem();
 * final GemFireStatSampler sampler = internalSystem.getStatSampler();          
 * sampler.addLocalStatListener(l, stats, statName);
 * </code>
 * 
 * @author sbawaska
 *
 */
public interface LocalStatListener {
  /**
   * Invoked when the value of a statistic has changed
   *
   * @param value
   *        The new value of the statistic 
   */
  public void statValueChanged( double value );
}
