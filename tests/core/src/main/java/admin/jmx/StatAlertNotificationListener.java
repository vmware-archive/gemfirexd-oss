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
package admin.jmx;

import hydra.Log;

import java.util.Date;

import javax.management.Notification;
import javax.management.NotificationListener;

public class StatAlertNotificationListener implements NotificationListener {

  protected long notificationCount;
  protected long notificationLimit;
  protected Date start = null;
  
  public StatAlertNotificationListener(long notificationLimit) {
   this.notificationLimit = notificationLimit;
   this.notificationCount = -1;
  }
  
  public long getNotificationCount() {
    return notificationCount;
  }
  
  public void handleNotification(Notification arg0, Object arg1) {
     
    if(((notificationCount+1)%notificationLimit) == 0) {
      if(start == null)
        start = new Date();
      else {
        Date now = new Date();
        Log.getLogWriter().info("StatAlertNotificationListener, Time required to accumulate notifications ="+(now.getTime() - start.getTime()));
        start = now;
      }
     }
    notificationCount = (notificationCount++)%notificationLimit;
  }
}
