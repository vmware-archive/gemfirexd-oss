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
package com.gemstone.gemfire.internal.util;

//import java.io.*;

/**
 * A {@link SerializableImpl} that implements an interface and has
 * some object state.
 *
 * @see DeserializerTest
 *
 * @author David Whitlock
 *
 * @since 2.0.3
 */
public class SerializableImplWithValue extends SerializableImpl 
  implements Valuable {

  /** This object's state */
  private Object value;

  /**
   * Zero-argument constructor
   */
  public SerializableImplWithValue() {

  }

  /**
   * Creates a new <code>SerializableImplWithValue</code> with a given
   * value
   */
  public SerializableImplWithValue(Object value) {
    this.value = value;
  }

  public Object getValue() {
    return this.value;
  }

  public void setValue(Object value) {
    this.value = value;
  }
}
