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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.*;

/**
 * Singleton token indicating that all interest registrations should be removed.
 *
 * @author Darrel Schneider
 *
 */
public class UnregisterAllInterest implements Serializable {
  private static final long serialVersionUID = 5026160621257178459L;
  private static final UnregisterAllInterest SINGLETON = new UnregisterAllInterest();
  /**
   * Return the only instance of this class.
   */
  public static final UnregisterAllInterest singleton() {
    return SINGLETON;
  }
  
  /** Creates a new instance of UnregisterAllInterest */
  private UnregisterAllInterest() {
  }
  
  @Override
  public String toString() {
    return "UNREGISTER ALL INTEREST";
  }
  
  private Object readResolve() throws ObjectStreamException {
    return SINGLETON;
  }
}
