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
package com.gemstone.gemfire.admin.jmx;

import javax.management.Notification;
import javax.management.NotificationListener;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.jmx.internal.AdminDistributedSystemJmxImpl;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

public class AdminDistributedSystemDisconnectDUnitTest extends AdminDUnitTestCase {
	
	public AdminDistributedSystemDisconnectDUnitTest(String name) {
	 super(name); 	
	}
	
	@Override
	protected boolean isJMX() {
   	  return true;
	}
	
	@Override
	protected boolean isRMI() {
	  return true; 	
	}
	
	public void testDisconnectListenerForAdminDistributedSystem() throws Exception {	 
	 assertTrue((this.tcSystem != null) && (this.tcSystem.isConnected()));
	 
	 DisconnectNotificationListener listener = new DisconnectNotificationListener();
	 
	 JMXAdminDistributedSystem sys = (JMXAdminDistributedSystem)this.tcSystem;
	 sys.addJMXNotificationListener(listener);
	 
	 
	 Host host = Host.getHost(0);
	 VM vm = host.getVM(AGENT_VM);
	 
     vm.invoke(new SerializableRunnable("disconnect from distributed system") {
         public void run() {          
          InternalDistributedSystem system = InternalDistributedSystem.getAnyInstance();
          system.disconnect();        	 
         }
       });
	 
     int attempts = 0;
     
     while (attempts < 5 && !listener.isNotifReceived()) {
       pause(1000);
       attempts++;
     }
     
     hydra.Log.getLogWriter().info("AdminDistributedSystem disconnect event received ::"+listener.isNotifReceived());
     assertTrue(listener.isNotifReceived());
     
     	 
     sys.removeJMXNotificationListener(listener);	 
	}
	
	
	static class DisconnectNotificationListener implements NotificationListener {
		private volatile boolean notifReceived = false;

		public void handleNotification(Notification notification,
				Object handback) {
			if (AdminDistributedSystemJmxImpl.NOTIF_ADMIN_SYSTEM_DISCONNECT
					.equals(notification.getType())) {
				this.notifReceived = true;
				hydra.Log.getLogWriter().info("Received AdminDistributedSystem disconnect event...");
			}
		}

		public boolean isNotifReceived() {
			return notifReceived;
		}
	}
}
