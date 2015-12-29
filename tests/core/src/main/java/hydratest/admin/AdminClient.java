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
package hydratest.admin;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.*;
import com.gemstone.gemfire.admin.jmx.*;
import hydra.*;

public class AdminClient {
  public static void connectToNonAdminDS() {
    DistributedSystemHelper.connect();
  }

  public static void connectToAdminDS() {
    DistributedSystemHelper.connectAdmin(ConfigPrms.getAdminConfig());
  }

  public static void startConnectedAgent() {
    AgentHelper.startConnectedAgent(ConfigPrms.getAgentConfig());
  }

  public static void checkEmail() {
    String tg = RemoteTestModule.getCurrentThread().getThreadGroupName();
    AdminDescription ad = TestConfig.getInstance().getAdminDescription(tg);
    checkEmailNotificationEnabled(tg, ad);
    checkEmailNotificationFrom(tg, ad);
    checkEmailNotificationHost(tg, ad);
    checkEmailNotificationToList(tg, ad);
  }
  private static void checkEmailNotificationEnabled(String tg,
                                                    AdminDescription ad) {
    boolean expected = AdminPrms.getExpectedEmailNotificationEnabled();
    Log.getLogWriter().info(tg + ":\n" + ad);
    boolean value = ad.getEmailNotificationEnabled().booleanValue();
    if (value != expected) {
      String s = tg + " expected emailNotificationEnabled " + expected
                    + ", got " + value; 
      throw new HydraRuntimeException(s);
    }
  }
  private static void checkEmailNotificationFrom(String tg,
                                                 AdminDescription ad) {
    String expected = AdminPrms.getExpectedEmailNotificationFrom();
    String value = ad.getEmailNotificationFrom();
    if (!value.equals(expected)) {
      String s = tg + " expected emailNotificationFrom " + expected
                    + ", got " + value; 
      throw new HydraRuntimeException(s);
    }
  }
  private static void checkEmailNotificationHost(String tg,
                                                 AdminDescription ad) {
    String expected = HostHelper.getCanonicalHostName(EnvHelper.convertHostName(AdminPrms.getExpectedEmailNotificationHost()));
    String value = ad.getEmailNotificationHost();
    if (!value.equals(expected)) {
      String s = tg + " expected emailNotificationHost " + expected
                    + ", got " + value; 
      throw new HydraRuntimeException(s);
    }
  }
  private static void checkEmailNotificationToList(String tg,
                                                   AdminDescription ad) {
    String expected = ad.getEmailNotificationToList(AdminPrms.getExpectedEmailNotificationToList());
    String value = ad.getEmailNotificationToList();
    if (!value.equals(expected)) {
      String s = tg + " expected emailNotificationToList " + expected
                    + ", got " + value; 
      throw new HydraRuntimeException(s);
    }
  }
}
