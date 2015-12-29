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
import com.gemstone.gemfire.admin.ManagedEntityConfig;
import com.gemstone.gemfire.internal.Assert;

import javax.management.*;

/**
 * A <code>ManagedEntityConfig</code> that delegates its work to JMX.
 *
 * @author David Whitlock
 * @since 4.0
 */
public abstract class JMXManagedEntityConfig extends JMXAdminImpl
  implements ManagedEntityConfig {

  /**
   * Creates a new <code>JMXManagedEntityConfig</code> 
   */
  protected JMXManagedEntityConfig(MBeanServerConnection mbs,
                                   ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Handles the given exception and will throw the appropriate
   * wrapped runtime exception.
   */
  protected RuntimeException handleException(Exception ex) {
    for (Throwable thr = ex; thr != null; thr = thr.getCause()) {
      if (thr instanceof MBeanException) {
        continue;

      } else if (thr instanceof RuntimeException) {
        return (RuntimeException) thr;
      }
    }

    String s = "While invoking a JMX operation";
    return new InternalGemFireException(s, ex);
  }

  public String getHost() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "host");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  /**
   * Sets the name of the host on which the managed entity will run.
   */
  public void setHost(String host) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("host", host));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getWorkingDirectory() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "workingDirectory");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setWorkingDirectory(String dir) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("workingDirectory", dir));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getProductDirectory() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "productDirectory");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setProductDirectory(String dir) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName, 
                            new Attribute("productDirectory", dir));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getRemoteCommand() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "remoteCommand");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void setRemoteCommand(String remoteCommand) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("remoteCommand", remoteCommand));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void validate() {
    throw new UnsupportedOperationException("not yet implemented");
  }

  public Object clone() throws CloneNotSupportedException {
    throw new UnsupportedOperationException("not yet implemented");
  }


}
