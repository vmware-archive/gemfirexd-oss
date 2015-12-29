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
package regions.validate;

/**
 * A <code>Value</code> that indicates that a region entry is in the
 * process of being changed.  It is used as a marker to ensure that
 * only one thread at a time can change the value of an entry.
 *
 * @author David Whitlock
 * @since 3.5
 */
public class InUse extends Value {

  /** The singleton instance of <code>InUse</code> */
  public static final InUse singleton = new InUse();

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates the singleton instance of <code>InUse</code>
   */
  private InUse() {

  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * The singleton <code>InUse</code> is only equal to itself.
   */
  public boolean equals(Object o) {
    return singleton == o;
  }

  public String toString() {
    return "InUse";
  }

  private Object readResolve() throws java.io.ObjectStreamException {
    return singleton;
  }

}
