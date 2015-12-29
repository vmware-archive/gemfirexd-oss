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
package com.gemstone.gemfire.admin;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import java.util.Properties;

/**
 * A little program that logs messages of varying severity.  It is
 * used to test the alert level functionality of the admin API and
 * console. 
 *
 * @author David Whitlock
 * @since 3.5
 */
public class SendAlerts {

  public static void main(String[] args) throws InterruptedException {
    DistributedSystem system =
      DistributedSystem.connect(new Properties());
    LogWriter logger = system.getLogWriter();

    for (int i = 0; i < 100; i++) {
      logger.finest("Finest message");
      Thread.sleep(500);
      logger.finer("Finer message");
      Thread.sleep(500);
      logger.fine("Fine message");
      Thread.sleep(500);
      logger.config("Config message");
      Thread.sleep(500);
      logger.info("Info message");
      Thread.sleep(500);
      logger.warning("Warning message");
      Thread.sleep(500);
      logger.severe("Severe message");
      Thread.sleep(500);
    }
  }

}
