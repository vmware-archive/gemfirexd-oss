/*
   Derby Classname com.pivotal.gemfirexd.internal.iapi.services.jmx.ManagementService
  
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
 
       http://www.apache.org/licenses/LICENSE-2.0
 
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

/*
 * Changes for GemFireXD distributed data platform.
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.iapi.services.jmx;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.Module;
import com.pivotal.gemfirexd.internal.mbeans.ManagementMBean;


/**
* This interface represents a Management Service. An implementation of this 
* service is started by the Derby monitor if the system property gemfirexd.system.jmx has
* been set. The following services are provided:
* 
*	<li> Create and start an instance of MBean server to register MBeans.
*       <li> Create managed beans (MBeans) to instrument derby resources for
*            management and monitoring.
* 
* The following code can be used to locate an instance of this service
* if running.
*
* ManagementService ms = (ManagementService)
*        Monitor.getSystemModule(Module.JMX);
*
*/
public interface ManagementService extends ManagementMBean {
   
    /**
     * The domain for all of derby's mbeans: com.pivotal.gemfirexd.internal
     */
    public static final String DERBY_JMX_DOMAIN = "com.pivotal.gemfirexd.internal";
    
    /**
     * Registers an MBean with the MBean server.
     * The mbean will be unregistered automatically when Derby shuts down.
     * 
     * @param bean The MBean to wrap with a StandardMBean and register
     * @param beanInterface The management interface for the MBean.
     * @param keyProperties The String representation of the MBean's key properties,
     * they will be added into the ObjectName with Derby's domain. Key
     * type should be first with a short name for the bean, typically the
     * class name without the package.
     * 
     * @return An idenitifier that can later be used to unregister the mbean.
     */
    public Object registerMBean(Object bean,
            Class beanInterface,
            String keyProperties)
            throws StandardException;
    
    /**
     * Unregister a mbean previously registered with registerMBean.
     * 
     * @param mbeanIdentifier An identifier returned by registerMBean.
     * @throws StandardException Error unregistering bean.
     */
    public void unregisterMBean(Object mbeanIdentifier);
}
