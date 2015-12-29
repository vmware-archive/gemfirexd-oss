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

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.DisconnectListener;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * Used to create and configure a {@link GfManagerAgent}.
 */
public class GfManagerAgentConfig {

  /** 
   * Constructs a GfManagerAgentConfig given the transport it should
   * use to connect to the remote systems and the LogWriterI18n to use for
   * logging messages.
   */
  public GfManagerAgentConfig(String displayName, TransportConfig transport,
                              LogWriterI18n logger, int level,
                              AlertListener listener,DisconnectListener  disconnectListener) {
    this.displayName = displayName;
    this.transport = transport;
    this.logger = logger;
    this.alertLevel = level;
    this.alertListener = listener;
    this.disconnectListener = disconnectListener;
  }

  public GfManagerAgentConfig(String displayName,
                              TransportConfig transport,
                              LogWriterI18n logger) {
    this.displayName = displayName;
    this.transport = transport;
    this.logger = logger;
    this.alertLevel = 0;
    this.alertListener = null;
    this.disconnectListener = null;
  }
  
  /**
   * Returns the communication transport configuration.
   */
  public TransportConfig getTransport() {
    return this.transport;
  }
  /**
   * Returns the log writer
   */
  public LogWriterI18n getLogger(){
    return logger;
  }
  /**
   * Returns the alert level
   */
  public int getAlertLevel() {
    return this.alertLevel;
  }
  /**
   * Returns the alert listener
   */
  public AlertListener getAlertListener() {
    return this.alertListener;
  }
  /**
   * Returns the display name
   */
  public String getDisplayName() {
    return this.displayName;
  }
  
  public DisconnectListener getDisconnectListener() {
	return disconnectListener;
  }
  
  private TransportConfig transport;
  private LogWriterI18n logger;
  private int alertLevel;
  private AlertListener alertListener;
  private String displayName;
  private DisconnectListener  disconnectListener;
}
