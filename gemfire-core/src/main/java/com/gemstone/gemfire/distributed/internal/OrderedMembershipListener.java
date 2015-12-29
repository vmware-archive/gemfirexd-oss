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

package com.gemstone.gemfire.distributed.internal;

/**
 * Extension to {@link MembershipListener} interface to provide an ordering of
 * {@link MembershipListener} invocations. Instances of this interface are
 * always invoked before non-ordered membership listeners and in the order
 * defined by the {@link OrderedMembershipListener#order()} method.
 * 
 * Every instance must return a unique order() number since this is used as the
 * key into the ordered map holding all instances of
 * {@link OrderedMembershipListener}.
 */
public interface OrderedMembershipListener extends MembershipListener {

  public static final int O_TXMANAGER = 1;

  /**
   * An order number for this instance. An instance returning a smaller value
   * will be invoked before those with larger values. All order numbers being
   * used must be listed in this interface for reference.
   */
  public int order();
}
