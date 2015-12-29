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
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;

/**
 * This class actually distribute the notification with the help of the actual
 * broadcaster proxy.
 * 
 * @author rishim
 * 
 */

public class NotificationHubClient {

  /**
   * proxy factory
   */
	private MBeanProxyFactory proxyFactory;
	/**
	 * logger
	 */
	private LogWriterI18n logger;

	protected NotificationHubClient(MBeanProxyFactory proxyFactory) {
		this.proxyFactory = proxyFactory;
		logger = InternalDistributedSystem.getLoggerI18n();
	}

	/**
	 * send the notification to actual client
	 * on the Managing node VM
	 * 
	 * it does not throw any exception. it will capture all
	 * exception and log a warning
	 * @param event
	 */
	public void sendNotification(EntryEvent<NotificationKey, Notification> event) {

		NotificationBroadCasterProxy notifBroadCaster;
		try {

      notifBroadCaster = proxyFactory.findProxy(event.getKey().getObjectName(),
          NotificationBroadCasterProxy.class);
			// Will return null if the Bean is filtered out.
			if (notifBroadCaster != null) {
				notifBroadCaster.sendNotification(event.getNewValue());
			}

		} catch (Exception e) {
		  if (logger.fineEnabled()) {
		    logger.fine(" NOTIFICATION Not Done " + e);
		  }		  
      logger.warning(e);
    }

	}

}
