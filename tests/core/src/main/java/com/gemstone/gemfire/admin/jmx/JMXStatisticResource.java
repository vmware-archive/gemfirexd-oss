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
//import java.util.Properties;
//import java.net.InetAddress;
import javax.management.*;

/**
 * An implementation of <code>StatisticResource</code> that
 * communicates via JMX.
 *
 * @author Kirk Lund
 */
class JMXStatisticResource extends JMXAdminImpl
  implements StatisticResource {
    
  /**
   * Creates a new <code>JMXStatisticResource</code> with the given MBean
   * server and object name.
   */
  JMXStatisticResource(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  public String getName() {//attr
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "name");
    } catch (Exception ex) {
      String s = "While calling getName";
      throw new InternalGemFireException(s, ex);
    }
  }

  public String getDescription() {//attr
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "description");
    } catch (Exception ex) {
      String s = "While calling getDescription";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public String getType() {//attr
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "type");
    } catch (Exception ex) {
      String s = "While calling getType";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public String getOwner() {//attr
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "owner");
    } catch (Exception ex) {
      String s = "While calling getOwner";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public long getUniqueId() {
    throw new UnsupportedOperationException("not implemented");
  }
  
  public Statistic[] getStatistics() {//op
    try {
      Assert.assertTrue(this.objectName != null);
      return (Statistic[]) this.mbs.invoke(this.objectName, "getStatistics",
                                        new Object[0], new String[0]);
    } catch (Exception ex) {
      String s = "While calling getStatistics";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public void refresh() throws com.gemstone.gemfire.admin.AdminException {//op
    try {
      this.mbs.invoke(this.objectName, "refresh",
                      new Object[0], new String[0]);
    } catch (Exception ex) {
      String s = "While calling refresh";
      throw new InternalGemFireException(s, ex);
    }
  }
  
}

