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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.internal.Assert;

import javax.management.*;

/**
 * An implementation of <code>DistributedSystemHealthConfig</code> that
 * communicates via JMX.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
class JMXDistributedSystemHealthConfig extends JMXAdminImpl
  implements DistributedSystemHealthConfig {

  /**
   * Creates a new <code>JMXDistributedSystemHealthConfig</code>
   */
  JMXDistributedSystemHealthConfig(MBeanServerConnection mbs,
                                   ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  //////////////////////  Instance Methods  //////////////////////

  public long getMaxDepartedApplications() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value =
        (Long) this.mbs.getAttribute(this.objectName,
                                     "maxDepartedApplications");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxDepartedApplications";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxDepartedApplications(long maxDepartedApplications) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxDepartedApplications",
                                          new Long(maxDepartedApplications)));

    } catch (Exception ex) {
      String s = "While setting the maxDepartedApplications";
      throw new InternalGemFireException(s, ex);
    }
  }
}
