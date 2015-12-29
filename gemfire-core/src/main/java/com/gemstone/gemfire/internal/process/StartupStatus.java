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
package com.gemstone.gemfire.internal.process;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.util.LogService;
import com.gemstone.org.jgroups.util.StringId;

/**
 * Extracted from LogWriterImpl and changed to static.
 *
 * @author Kirk Lund
 */
public class StartupStatus {

  private static final LogWriter logger = LogService.logWriter();

  /**
   * A listener which can be registered to be informed of startup events
   */
  private static volatile StartupStatusListener listener;

  /**
   * Writes a message to this writer.
   * If a startup listener is registered,
   * the message will be written to the listener as well
   * to be reported to a user.
   *
   * @since 7.0
   */
  public static synchronized void startup(String message) {
    if (listener != null) {
      listener.setStatus(message);
    }
    logger.info(message);
  }

  /**
   * Writes both a message and exception to this writer.
   * If a startup listener is registered,
   * the message will be written to the listener as well
   * to be reported to a user.
   *
   * @since 7.0
   */
  public static void startup(StringId msgID, Object[] params) {
    String message = msgID.toLocalizedString(params);
    startup(message);
  }

  public static synchronized void setListener(StartupStatusListener listener) {
    StartupStatus.listener = listener;
  }

  public static synchronized StartupStatusListener getStartupListener() {
    return StartupStatus.listener;
  }

  public static synchronized void clearListener() {
    StartupStatus.listener = null;
  }
}
