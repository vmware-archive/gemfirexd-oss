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
package com.gemstone.gemfire.management.internal;

import javax.management.Notification;
import javax.management.NotificationBroadcaster;

/**
 * This interface is to extend the functionality of NotificationBroadcaster.
 * It enables proxies to send notifications on their own as 
 * proxy implementations have to implement the sendNotification method.
 * 
 * 
 * @author rishim
 *
 */

public interface NotificationBroadCasterProxy extends NotificationBroadcaster{
  /**
   * send the notification to registered clients
   * @param notification
   */
	public void sendNotification(Notification notification);
}
