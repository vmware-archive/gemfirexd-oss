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
package com.gemstone.gemfire.internal;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

/**
 * <P>This class is an Ant task for selecting a random available port
 * on through which a socket or JGroups channel may be established.
 * To use this task in a build file, you must set up a
 * <code>taskdef</code> such as:</P>
 *
 * <PRE>
 * &lt;taskdef name="available-port" 
 *    classname="com.gemstone.gemfire.internal.AvailablePortTask"
 *    classpath="${gemfire.home}/lib/gemfire.jar" /&gt;
 * </PRE>
 *
 * <HR>
 *
 * <h2><a name="enhance">Available Port</a></h2>
 *
 * <h3>Description</h3>
 *
 * <P>Uses the {@link AvailablePort} class to find an available port
 * on which a socket or JGroups channel may be created.  The port
 * number is assigned into an Ant property. </P>
 *
 * <h3>Parameters</h3>
 *
 * <table border="1" cellpadding="2" cellspacing="0">
 *  <tr>
 *    <td valign="top"><b>Attribute</b></td>
 *    <td valign="top"><b>Description</b></td>
 *    <td align="center" valign="top"><b>Required</b></td>
 *  </tr>
 *  <tr>
 *    <td valign="top">protocol</td>
 *    <td valign="top">The networking protocol for the desired port
 *        (either <TT>socket</TT> or <TT>jgroups</TT></td>
 *    <td align="center" valign="top">Yes</td>
 *  </tr>
 *  <tr>
 *    <td valign="top">portProperty</td>
 *    <td valign="top">The system property whose value will be set to
 *                     the available port number.</td>
 *    <td align="center" valign="top">Yes</td>
 *  </tr>
 * </table>
 *
 * <h3>Examples</h3>
 *
 * <PRE>
 * &lt;available-port protocol=&quot;jgroups&quot; 
 *                    portProperty=&quot;multicast.port&quot;/&gt;
 * </PRE>
 *
 * <P>searches for an available port on which to connect to JGroups</P>
 *
 * @see AvailablePort
 *
 * @author David Whitlock
 *
 */
public class AvailablePortTask extends Task {

  private String protocolString = null;
  private String property = null;

  ///////////////////////  Instance Methods  ///////////////////////

  /**
   * Executes this task.  Checks to make sure the protocol and port
   * property have been set.  The just invokes {@link AvailablePort}
   * appropriately.
   */
  @Override
  public void execute() throws BuildException {
    Project proj = this.getProject();

    int protocol;

    if (protocolString == null) {
      throw new BuildException(LocalizedStrings.AvailablePortTask_MISSING_PROTOCOL.toLocalizedString());

    } else if (protocolString.equalsIgnoreCase("JGROUPS")) {
      protocol = AvailablePort.JGROUPS;

    } else if (protocolString.equalsIgnoreCase("SOCKET")) {
      protocol = AvailablePort.SOCKET;

    } else {
      throw new BuildException(LocalizedStrings.AvailablePortTask_UNKNOWN_PROTOCOL_0.toLocalizedString(protocolString));
    }

    if (property == null) {
      throw new BuildException(LocalizedStrings.AvailablePortTask_MISSING_PORT_PROPERTY.toLocalizedString());
    }

    // Because the XML parsing code relies on the fact that certain
    // configuration files can be located from the current context
    // loader, we set the context class loader to be the class loader
    // with all of the GemFire classes on it.
    ClassLoader oldCl =
      Thread.currentThread().getContextClassLoader();
    ClassLoader cl = this.getClass().getClassLoader();
    Thread.currentThread().setContextClassLoader(cl);
    try {
      int port = AvailablePort.getRandomAvailablePort(protocol);
      proj.setProperty(property, String.valueOf(port));

    } finally {
      Thread.currentThread().setContextClassLoader(oldCl);
    }
  }

  public void setPortproperty(String property) {
    this.property = property;
  }

  public void setProtocol(String protocol) {
    this.protocolString = protocol;
  }

}
