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
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.Assert;

import java.io.File;
import java.util.Set;
import javax.management.*;

/**
 * An implementation of the <code>SystemMemberRegion</code> interface
 * that communicates via JMX.
 *
 * @author David Whitlock
 * @since 3.5
 */
class JMXSystemMemberRegion extends JMXAdminImpl
  implements SystemMemberRegion {

  /**
   * Creates a new <code>JMXSystemMemberRegion</code> with the given
   * MBean server and object name.
   */
  JMXSystemMemberRegion(MBeanServerConnection mbs, ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  /**
   * Handles an exception thrown when invoking MBean method by
   * unwrapping it appropriately.
   *
   * @return The appropriate exception to be thrown
   */
  private RuntimeException handleException(Exception ex) {
    if (ex instanceof MBeanException) {
      MBeanException mbe   = (MBeanException) ex;
      Throwable      cause = mbe.getCause();
      
      if (cause instanceof RegionNotFoundException) {
        return (RegionNotFoundException) cause;
      } else if (cause instanceof RegionDestroyedException) {
        return (RegionDestroyedException) cause;
      } else if (cause instanceof ServiceNotFoundException) {
        // MBeanServerConnection interface returns ServiceNotFoundException
        // SystemMemberRegion interface needs to throw RegionNotFoundException
        return new RegionNotFoundException("MBeanServerConnection.getAttribute() caught " + cause.getMessage());
      }
    } else if (ex instanceof InstanceNotFoundException) {
      // MBeanServerConnection interface returns InstanceNotFoundException
      // SystemMemberRegion interface needs to throw RegionNotFoundException
      return new RegionNotFoundException("MBeanServerConnection.getAttribute() caught " + ex.getMessage());
    }

    String s = "While calling MBeanServerConnection.getAttribute()";
    return new InternalGemFireException(s, ex);
  }

  public String getName() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "name");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getFullPath() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "fullPath");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public Set getSubregionNames() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (Set) this.mbs.getAttribute(this.objectName, "subregionNames");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public Set getSubregionFullPaths() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (Set) this.mbs.getAttribute(this.objectName, "subregionFullPaths");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getUserAttribute() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "userAttribute");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getCacheLoader() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "cacheLoader");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getCacheWriter() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "cacheWriter");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public EvictionAttributes getEvictionAttributes() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (EvictionAttributes) this.mbs.getAttribute(this.objectName, "evictionAttributes");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public MembershipAttributes getMembershipAttributes() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (MembershipAttributes) this.mbs.getAttribute(this.objectName, "membershipAttributes");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public SubscriptionAttributes getSubscriptionAttributes() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (SubscriptionAttributes) this.mbs.getAttribute(this.objectName, "subscriptionAttributes");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getCacheListener() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "cacheListener");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String[] getCacheListeners() {
    try {
      Assert.assertTrue(this.objectName != null);
      String[] params = {}, signature = {};
      return (String[]) this.mbs.invoke(this.objectName, "getCacheListeners", params, signature);
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean getEarlyAck() {
    try {
      Assert.assertTrue(this.objectName != null);
      Boolean value = (Boolean)this.mbs.getAttribute(this.objectName, "earlyAck");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getKeyConstraint() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "keyConstraint");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getValueConstraint() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "valueConstraint");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getRegionTimeToLiveTimeLimit() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "regionTimeToLiveTimeLimit");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public ExpirationAction getRegionTimeToLiveAction() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (ExpirationAction) this.mbs.getAttribute(this.objectName, "regionTimeToLiveAction");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getEntryTimeToLiveTimeLimit() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "entryTimeToLiveTimeLimit");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public ExpirationAction getEntryTimeToLiveAction() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (ExpirationAction) this.mbs.getAttribute(this.objectName, "entryTimeToLiveAction");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public String getCustomEntryTimeToLive() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "customEntryTimeToLive");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public int getRegionIdleTimeoutTimeLimit() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "regionIdleTimeoutTimeLimit");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public ExpirationAction getRegionIdleTimeoutAction() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (ExpirationAction) this.mbs.getAttribute(this.objectName, "regionIdleTimeoutAction");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getEntryIdleTimeoutTimeLimit() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "entryIdleTimeoutTimeLimit");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public ExpirationAction getEntryIdleTimeoutAction() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (ExpirationAction) this.mbs.getAttribute(this.objectName, "entryIdleTimeoutAction");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public String getCustomEntryIdleTimeout() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName, "customEntryIdleTimeout");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public MirrorType getMirrorType() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (MirrorType) this.mbs.getAttribute(this.objectName, "mirrorType");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public DataPolicy getDataPolicy() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (DataPolicy) this.mbs.getAttribute(this.objectName, "dataPolicy");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public Scope getScope() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (Scope) this.mbs.getAttribute(this.objectName, "scope");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getInitialCapacity() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "initialCapacity");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public float getLoadFactor() {
    try {
      Assert.assertTrue(this.objectName != null);
      Float value = (Float) this.mbs.getAttribute(this.objectName, "loadFactor");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getConcurrencyLevel() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "concurrencyLevel");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean getConcurrencyChecksEnabled() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((Boolean)this.mbs.getAttribute(this.objectName, "concurrencyChecksEnabled")).booleanValue();
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean getStatisticsEnabled() {
    try {
      Assert.assertTrue(this.objectName != null);
      Boolean value = (Boolean) this.mbs.getAttribute(this.objectName, "statisticsEnabled");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public boolean getPersistBackup() {
    try {
      Assert.assertTrue(this.objectName != null);
      Boolean value = (Boolean) this.mbs.getAttribute(this.objectName, "persisteBackup");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public DiskWriteAttributes getDiskWriteAttributes() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (DiskWriteAttributes) this.mbs.getAttribute(this.objectName, "diskWriteAttributes");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public File[] getDiskDirs() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (File[]) this.mbs.getAttribute(this.objectName, "diskDirs");
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getEntryCount() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "entryCount");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public int getSubregionCount() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = (Integer) this.mbs.getAttribute(this.objectName, "subregionCount");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public long getLastModifiedTime() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = (Long) this.mbs.getAttribute(this.objectName, "lastModifiedTime");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public long getLastAccessedTime() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = (Long) this.mbs.getAttribute(this.objectName, "lastAccessedTime");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public long getHitCount() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = (Long) this.mbs.getAttribute(this.objectName, "hitCount");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public long getMissCount() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = (Long) this.mbs.getAttribute(this.objectName, "missCount");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public float getHitRatio() {
    try {
      Assert.assertTrue(this.objectName != null);
      Float value = (Float) this.mbs.getAttribute(this.objectName, "hitRatio");
      return value;
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }
  
  public PartitionAttributes getPartitionAttributes() {
    try {
      Assert.assertTrue(this.objectName != null);
      return ((PartitionAttributes) this.mbs.getAttribute(this.objectName, "partitionAttributes"));
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public void refresh() {
    try {
      this.mbs.invoke(this.objectName, "refresh",
                      new Object[0], new String[0]);
      
    } catch (Exception ex) {
      throw handleException(ex);
    }
  }

  public SystemMemberRegion createSubregion(String name,
                                            RegionAttributes attrs)
    throws AdminException {

    throw new UnsupportedOperationException("Not implemented yet");
  }

}
