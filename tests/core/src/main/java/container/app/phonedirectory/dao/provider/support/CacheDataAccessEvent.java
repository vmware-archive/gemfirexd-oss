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
package container.app.phonedirectory.dao.provider.support;

import container.app.dao.DataAccessType;
import container.app.dao.support.DataAccessEvent;
import container.app.util.Assert;

public class CacheDataAccessEvent extends DataAccessEvent {
  
  private static final long serialVersionUID = 113151L;

  private final Object key;
  private final Object oldValue;

  public CacheDataAccessEvent(final Object source, final DataAccessType op, final Object key, final Object value) {
    this(source, op, key, null, value);
  }

  public CacheDataAccessEvent(final Object source, final DataAccessType op, final Object key, final Object oldValue, final Object newValue) {
    super(source, op, newValue);
    Assert.notNull(key, "The key under which the value was cached cannot be null!");
    this.key = key;
    this.oldValue = oldValue;
  }

  public <T> T getKey() {
    return (T) this.key;
  }

  public <T> T getNewValue() {
    return this.<T>getValue();
  }

  public <T> T getOldValue() {
    return (T) this.oldValue;
  }

}
