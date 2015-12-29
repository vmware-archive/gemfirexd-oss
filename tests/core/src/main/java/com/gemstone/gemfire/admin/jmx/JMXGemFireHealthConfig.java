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

import java.util.Set;
import javax.management.*;

/**
 * An implementation of <code>GemFireHealthConfig</code> that
 * communicates via JMX.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
class JMXGemFireHealthConfig extends JMXAdminImpl
  implements GemFireHealthConfig {

  /**
   * Creates a new <code>JMXDistributedSystemHealthConfig</code>
   */
  JMXGemFireHealthConfig(MBeanServerConnection mbs,
                         ObjectName objectName) {
    super(mbs, objectName);
    Assert.assertTrue(this.objectName != null);
  }

  ////////////////////  GemFireHealthConfig  ////////////////////

  public String getHostName() {
    try {
      Assert.assertTrue(this.objectName != null);
      return (String) this.mbs.getAttribute(this.objectName,
                                            "hostName");

    } catch (Exception ex) {
      String s = "While getting the hostName";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setHealthEvaluationInterval(int interval) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("healthEvaluationInterval",
                                          new Integer(interval)));

    } catch (Exception ex) {
      String s = "While setting the healthEvaluationInterval";
      throw new InternalGemFireException(s, ex);
    }
  }

  public int getHealthEvaluationInterval() {
    try {
      Assert.assertTrue(this.objectName != null);
      Integer value = 
        (Integer) this.mbs.getAttribute(this.objectName,
                                        "healthEvaluationInterval");
      return value.intValue();

    } catch (Exception ex) {
      String s = "While getting the healthEvaluationInterval";
      throw new InternalGemFireException(s, ex);
    }
  }

  /////////////////  SystemManagerHealthConfig  //////////////////

  public long getMinObjectsRecycled() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "minObjectsRecycled");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the healthEvaluationInterval";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMinObjectsRecycled(long minObjectsRecycled) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("minObjectsRecycled",
                                          new Long(minObjectsRecycled)));

    } catch (Exception ex) {
      String s = "While setting the minObjectsRecycled";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMinObjectsCompacted(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "minObjectsCompacted");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the minObjectsCompacted";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMinObjectsCompacted(long minObjectsCompacted) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("minObjectsCompacted",
                                          new Long(minObjectsCompacted)));

    } catch (Exception ex) {
      String s = "While setting the minObjectsCompacted";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxObjectsFaulted(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxObjectsFaulted");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxObjectsFaulted";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxObjectsFaulted(long maxObjectsFaulted) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxObjectsFaulted",
                                          new Long(maxObjectsFaulted)));

    } catch (Exception ex) {
      String s = "While setting the maxObjectsFaulted";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxThrottledObjects(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxThrottledObjects");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxThrottledObjects";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxThrottledObjects(long maxThrottledObjects) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxThrottledObjects",
                                          new Long(maxThrottledObjects)));

    } catch (Exception ex) {
      String s = "While setting the maxThrottledObjects";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxObjectTableEntries() {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxObjectTableEntries");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxObjectTableEntries";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxObjectTableEntries(long maxObjectTableEntries) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxObjectTableEntries",
                                          new Long(maxObjectTableEntries)));

    } catch (Exception ex) {
      String s = "While setting the maxObjectTableEntries";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxInstances(String className) {
    try {
      Assert.assertTrue(this.objectName != null);
      Long value =
        (Long) this.mbs.invoke(this.objectName,
                               "getMaxInstancesClasses",
                               new Object[] { className }, 
                               new String[] { String.class.getName() });
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the healthEvaluationInterval";
      throw new InternalGemFireException(s, ex);
    }
  }

  public String[] getMaxInstancesClassNames() {
    try {
      Set set =
        (Set) this.mbs.invoke(this.objectName,
                              "getMaxInstancesClasses",
                              new Object[0], new String[0]);
      return (String[]) set.toArray(new String[set.size()]);

    } catch (Exception ex) {
      String s = "While getting the healthEvaluationInterval";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxInstances(String className, long maxInstances) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.invoke(this.objectName,
                      "setMaxInstances",
                      new Object[] { className, new Long(maxInstances) }, 
                      new String[] { String.class.getName(), Long.class.getName() });

    } catch (Exception ex) {
      String s = "While setting the maxInstances";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxLogMessages(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxLogMessages");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxLogMessages";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxLogMessages(long maxLogMessages) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxLogMessages",
                                          new Long(maxLogMessages)));

    } catch (Exception ex) {
      String s = "While setting the maxLogMessages";
      throw new InternalGemFireException(s, ex);
    }
  }

//   public long getMinDiskSpace(){
//     try {
//       Long value = 
//         (Long) this.mbs.getAttribute(this.objectName,
//                                         "minDiskSpace");
//       return value.longValue();

//     } catch (Exception ex) {
//       String s = "While getting the minDiskSpace";
//       throw new InternalGemFireException(s, ex);
//     }
//   }

//   public void setMinDiskSpace(long minDiskSpace) {
//     try {
//       this.mbs.setAttribute(this.objectName,
//                             new Attribute("minDiskSpace",
//                                           new Long(minDiskSpace)));

//     } catch (Exception ex) {
//       String s = "While setting the minDiskSpace";
//       throw new InternalGemFireException(s, ex);
//     }
//   }

  public long getMaxDisconnects(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxDisconnects");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxDisconnects";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxDisconnects(long maxDisconnects) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxDisconnects",
                                          new Long(maxDisconnects)));

    } catch (Exception ex) {
      String s = "While setting the maxDisconnects";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxJavaObjects(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxJavaObjects");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxJavaObjects";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxJavaObjects(long maxJavaObjects) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxJavaObjects",
                                          new Long(maxJavaObjects)));

    } catch (Exception ex) {
      String s = "While setting the maxJavaObjects";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxVotingTime(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxVotingTime");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxVotingTime";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxVotingTime(long maxVotingTime) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxVotingTime",
                                          new Long(maxVotingTime)));

    } catch (Exception ex) {
      String s = "While setting the maxVotingTime";
      throw new InternalGemFireException(s, ex);
    }
  }

  //////////////////////  CacheHealthConfig  //////////////////////

  public long getMaxNetSearchTime(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxNetSearchTime");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxNetSearchTime";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxNetSearchTime(long maxNetSearchTime) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxNetSearchTime",
                                          new Long(maxNetSearchTime)));

    } catch (Exception ex) {
      String s = "While setting the maxNetSearchTime";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxLoadTime(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxLoadTime");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxLoadTime";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxLoadTime(long maxLoadTime) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxLoadTime",
                                          new Long(maxLoadTime)));

    } catch (Exception ex) {
      String s = "While setting the maxLoadTime";
      throw new InternalGemFireException(s, ex);
    }
  }

  public double getMinHitRatio(){
    try {
      Assert.assertTrue(this.objectName != null);
      Double value = 
        (Double) this.mbs.getAttribute(this.objectName,
                                        "minHitRatio");
      return value.doubleValue();

    } catch (Exception ex) {
      String s = "While getting the minHitRatio";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMinHitRatio(double minHitRatio) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("minHitRatio",
                                          new Double(minHitRatio)));

    } catch (Exception ex) {
      String s = "While setting the minHitRatio";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxEventQueueSize(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxEventQueueSize");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxEventQueueSize";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxEventQueueSize(long maxEventQueueSize) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxEventQueueSize",
                                          new Long(maxEventQueueSize)));

    } catch (Exception ex) {
      String s = "While setting the maxEventQueueSize";
      throw new InternalGemFireException(s, ex);
    }
  }

  //////////////////////  MemberHealthConfig  //////////////////////

  public long getMaxVMProcessSize(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxVMProcessSize");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxVMProcessSize";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxVMProcessSize(long size) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("size",
                                          new Long(size)));

    } catch (Exception ex) {
      String s = "While setting the size";
      throw new InternalGemFireException(s, ex);
    }
  }
  
  public long getMaxMessageQueueSize(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxMessageQueueSize");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxMessageQueueSize";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxMessageQueueSize(long maxMessageQueueSize) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxMessageQueueSize",
                                          new Long(maxMessageQueueSize)));

    } catch (Exception ex) {
      String s = "While setting the maxMessageQueueSize";
      throw new InternalGemFireException(s, ex);
    }
  }

  public long getMaxReplyTimeouts(){
    try {
      Assert.assertTrue(this.objectName != null);
      Long value = 
        (Long) this.mbs.getAttribute(this.objectName,
                                        "maxReplyTimeouts");
      return value.longValue();

    } catch (Exception ex) {
      String s = "While getting the maxReplyTimeouts";
      throw new InternalGemFireException(s, ex);
    }
  }

  public void setMaxReplyTimeouts(long maxReplyTimeouts) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxReplyTimeouts",
                                          new Long(maxReplyTimeouts)));

    } catch (Exception ex) {
      String s = "While setting the maxReplyTimeouts";
      throw new InternalGemFireException(s, ex);
    }
  }
  public double getMaxRetransmissionRatio() {
    try {
      Assert.assertTrue(this.objectName != null);
      Double value = 
        (Double) this.mbs.getAttribute(this.objectName,
                                        "maxRetransmissionRatio");
      return value.doubleValue();

    } catch (Exception ex) {
      String s = "While getting the maxRetransmissionRatio";
      throw new InternalGemFireException(s, ex);
    }
  }
  public void setMaxRetransmissionRatio(double value) {
    try {
      Assert.assertTrue(this.objectName != null);
      this.mbs.setAttribute(this.objectName,
                            new Attribute("maxRetransmissionRatio",
                                          new Double(value)));

    } catch (Exception ex) {
      String s = "While setting the maxRetransmissionRatio";
      throw new InternalGemFireException(s, ex);
    }
  }

}
