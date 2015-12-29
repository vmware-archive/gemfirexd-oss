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

import java.io.*;

/**
 * A {@link Serializable} class that is loaded by a class loader other
 * than the one that is used to load test classes.
 *
 * @see DeserializerTest
 *
 * @author David Whitlock
 *
 * @since 2.0.1
 */
public class SerializableImpl implements Serializable {

  /**
   * Creates a new <code>SerializableImpl</code>
   */
  public SerializableImpl() {

  }

}
