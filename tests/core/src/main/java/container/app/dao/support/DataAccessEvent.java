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
package container.app.dao.support;

import java.util.EventObject;

import container.app.dao.DataAccessType;
import container.app.util.Assert;

public class DataAccessEvent extends EventObject implements PersistentEvent {

  private static final long serialVersionUID = 917151L;

  private final DataAccessType op;
  
  private final Object value;

  public DataAccessEvent(final Object source, final DataAccessType op, final Object value) {
    super(source);

    Assert.notNull(op, "The CRUD operation must be specified!");
    Assert.notNull(value, "The value upon which the CRUD operation occurred cannot be null!");

    this.op = op;
    this.value = value;
  }

  public DataAccessType getCrudOperation() {
    return this.op;
  }

  public <T> T getValue() {
    return (T) this.value;
  }

}
