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

package com.pivotal.gemfirexd.internal.iapi.sql.compile;

import com.gemstone.gemfire.InternalGemFireError;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * Adaptor for {@link Visitor} to easily allow adding new methods to the
 * interface.
 * 
 * @author swale
 * @since 7.0
 */
public class VisitorAdaptor implements Visitor {

  /**
   * {@inheritDoc}
   */
  @Override
  public Visitable visit(Visitable node) throws StandardException {
    return node;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean stopTraversal() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean skipChildren(Visitable node) throws StandardException {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsDeltaMerge() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean initForDeltaState() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getAndResetDeltaState() {
    throw new InternalGemFireError("unexpected invocation");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Visitable mergeDeltaState(Object delta, Visitable node) {
    throw new InternalGemFireError("unexpected invocation");
  }
}
