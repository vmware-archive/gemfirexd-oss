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

import com.gemstone.gemfire.internal.Assert;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

/**
 * The abstract superclass of classes that provide JMX-based
 * implementation of the external {@link com.gemstone.gemfire.admin
 * admin} API.
 *
 * @author David Whitlock
 * @author Kirk Lund
 */
abstract class JMXAdminImpl {

  /** The MBean server used to operate on beans */
  protected MBeanServerConnection mbs;

  /** The name of the JMX Bean we delegate to */
  protected ObjectName objectName;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>JMXAdminImpl</code> with the given MBean
   * server and object name.
   */
  JMXAdminImpl(MBeanServerConnection mbs, ObjectName objectName) {
    this.mbs = mbs;
    this.objectName = objectName;
//    Assert.assertTrue(this.objectName != null);  Can't do this :-(
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the name of the MBean represented by this object
   */
  ObjectName getObjectName() {
    Assert.assertTrue(this.objectName != null);
    return this.objectName;
  }

}

