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

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.LogWriterImpl;

/**
 * An administration alert that is issued by a member of a GemFire
 * distributed system.  It is similar to a log message.
 *
 * @see AlertListener
 */
public interface Alert {
  /** The level at which this alert is issued */
  public int getLevel();

  /** The member of the distributed system that issued the alert */
  public GemFireVM getGemFireVM();

  /** The name of the <code>GemFireConnection</code> through which the
   * alert was issued. */
  public String getConnectionName();

  /** The id of the source of the alert (such as a thread in a VM) */
  public String getSourceId();

  /** The alert's message */
  public String getMessage();

  /** The time at which the alert was issued */
  public java.util.Date getDate();

  /**
   * Returns a InternalDistributedMember instance representing a member that is
   * sending (or has sent) this alert. Could be <code>null</code>.
   * 
   * @return the InternalDistributedMember instance representing a member that
   *         is sending/has sent this alert
   *
   * @since 6.5        
   */
  public InternalDistributedMember getSender();

  public final static int ALL = LogWriterImpl.ALL_LEVEL;
  public final static int OFF = LogWriterImpl.NONE_LEVEL;
  public final static int FINEST = LogWriterImpl.FINEST_LEVEL;
  public final static int FINER = LogWriterImpl.FINER_LEVEL;
  public final static int FINE = LogWriterImpl.FINE_LEVEL;
  public final static int CONFIG = LogWriterImpl.CONFIG_LEVEL;
  public final static int INFO = LogWriterImpl.INFO_LEVEL;
  public final static int WARNING = LogWriterImpl.WARNING_LEVEL;
  public final static int ERROR = LogWriterImpl.ERROR_LEVEL;
  public final static int SEVERE = LogWriterImpl.SEVERE_LEVEL;
}
