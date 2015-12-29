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
   
package com.gemstone.gemfire.internal.admin;

/**
 * Interface for those who want to be alerted of a change in value of
 * a statistic
 */
public interface StatListener {
  /**
   * Invoked when the value of a statistic has changed
   *
   * @param value
   *        The new value of the statistic
   * @param time
   *        The time at which the statistic's value change was
   *        detected 
   */
  public void statValueChanged( double value, long time );

  /**
   * Invoked when the value of a statistic has not changed
   *
   * @param time
   *        The time of the latest statistic sample
   */
  public void statValueUnchanged( long time );
}
