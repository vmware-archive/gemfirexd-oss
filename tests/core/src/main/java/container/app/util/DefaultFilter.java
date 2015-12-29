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
package container.app.util;

public final class DefaultFilter<T> implements Filter<T> {

  private static final boolean DEFAULT_ACCEPT_VALUE = true;

  private final boolean acceptValue;

  public DefaultFilter() {
    this(DEFAULT_ACCEPT_VALUE);
  }
  
  public DefaultFilter(final boolean acceptValue) {
    this.acceptValue = acceptValue;
  }

  public boolean accept(final T obj) {
    return this.acceptValue;
  }

}
