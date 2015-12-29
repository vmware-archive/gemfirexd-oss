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

package hydra;

import java.io.Serializable;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.regex.*;

/**
 *  This class provides objects used to describe client VMs.  These objects are
 *  integral to the implementation of the {@link ClientVmMgr} API, which allows
 *  hydra clients to stop and start client VMs.
 *  <p>
 *  The info objects are used to pass in descriptions of the source VM (the one
 *  calling the stop or start method) and the target VM (the one which will be
 *  stopped or started).  Information about the target VM can be specific enough
 *  to match only one VM or vague enough so that more than one VM might match
 *  (though only one will be chosen and acted upon).
 *  <p>
 *  Methods in the ClientVmMgr API return another info object containing full
 *  information about the selected target VM.
 */

public class ClientVmInfo implements Serializable {

  /////////////////////  Instance Fields   /////////////////////

  private Integer vmid;
  private String clientName;
  private String logicalHost;

  ///////////////////////  Constructors  ///////////////////////

  /**
   *  Creates an object describing this client VM, for use with the
   *  {@link ClientVmMgr} API.
   *  <p>
   *  For internal hydra use only.
   */
  protected ClientVmInfo() {
    try {
      setVmid( new Integer( RemoteTestModule.MyVmid ) );
      setClientName( RemoteTestModule.MyClientName );
      setLogicalHost( RemoteTestModule.MyLogicalHost );
    } catch( IllegalArgumentException e ) {
      throw new HydraInternalException( "Bad RTM settings", e );
    }
  }

  /**
   *  Creates an object describing a client VM, for use with the
   *  {@link ClientVmMgr} API.
   *
   *  @param vmid        The unique logical id for the client VM.
   *  @throws IllegalArgumentException if the info provided does not exist.
   */
  public ClientVmInfo(int vmid) {
    setVmid(new Integer(vmid));
  }

  /**
   *  Creates an object describing a client VM, for use with the
   *  {@link ClientVmMgr} API.
   *
   *  @param vmid        The unique logical id for the client VM.  Use null
   *                     to allow a VM with any ID to match.
   *  @param clientName  One of the values in {@link hydra.ClientPrms#names}.
   *                     Use null to match any logical client name.
   *  @param logicalHost One of the values in {@link hydra.HostPrms#names}.
   *                     Use null to match any logical host name.
   *  @throws IllegalArgumentException if any of info provided does not exist.
   */
  public ClientVmInfo( Integer vmid, String clientName, String logicalHost ) {
    setVmid( vmid );
    setClientName( clientName );
    setLogicalHost( logicalHost );
  }

  /**
   * Creates an object describing a specific bridge server VM, for use with
   * the {@link ClientVmMgr} API.  To describe a more generic server, use
   * {@link ClientVmInfo(Integer,String,String)}.
   *
   * @param endpoint The {@link BridgeHelper.Endpoint} for the server.
   * @throws IllegalArgumentException if the endpoint is null.
   */
  public ClientVmInfo(BridgeHelper.Endpoint endpoint) {
    if (endpoint == null) {
      throw new IllegalArgumentException("endpoint cannot be null");
    }
    setVmid(new Integer(endpoint.getVmid()));
    setClientName(endpoint.getName());
  }

  ////////////////////////  Accessors  /////////////////////////

  /**
   *  @return Unique logical id for the client VM.
   */
  public Integer getVmid() {
    return this.vmid;
  }
  protected void setVmid( Integer vmid ) {
    this.vmid = vmid;
  }

  /**
   *  @return Logical name of the client VM from
   *          <code>hydra.ClientPrms-names</code>.
   */
  public String getClientName() {
    return this.clientName;
  }
  protected void setClientName( String clientName ) {
    this.clientName = clientName;
    if (clientName != null && !clientNameIsValid()) {
      String s = "Client name " + clientName + " not found";
      throw new IllegalArgumentException( s );
    }
  }
  private boolean clientNameIsValid() {
    Vector clientNames = TestConfig.getInstance().getClientNames();
    for (Iterator i = clientNames.iterator(); i.hasNext();) {
      if (clientNameMatches((String)i.next())) {
        return true;
      }
    }
    return false;
  }
  protected boolean clientNameMatches(String aClientName) {
    if (this.clientName.equals(aClientName)) { // have exact match
      return true;
    }
    if (this.clientName.indexOf("*") != -1) { // try pattern matching
      Pattern pattern = patternFor(this.clientName);
      if (pattern.matcher(aClientName).matches()) {
        return true;
      }
    }
    return false;
  }
  private Pattern patternFor(String s) {
    String result = new String();
    StringTokenizer tokenizer = new StringTokenizer(s, "*", true);
    while (tokenizer.hasMoreTokens()) {
      String token = tokenizer.nextToken();
      if (token.equals("*")) {
        result += ".";
      }
      result += token;
    }
    return Pattern.compile(result);
  }

  /**
   *  @return Logical name of the client VM's host from
   *          <code>hydra.HostPrms-names</code>.
   */
  public String getLogicalHost() {
    return this.logicalHost;
  }
  protected void setLogicalHost( String logicalHost ) {
    if ( logicalHost != null &&
      ! TestConfig.getInstance().getHostNames().contains( logicalHost ) ) {
      String s = "Host name " + logicalHost + " not found";
      throw new IllegalArgumentException( s );
    }
    this.logicalHost = logicalHost;
  }

  /////////////////////  Instance Methods  /////////////////////

  /**
   *  @return Stringified client VM information.
   */
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append( "vm_" );
    if ( this.vmid == null ) {
      buf.append( "*" );
    } else {
      buf.append( this.vmid );
    }
    buf.append( "_" );
    if ( this.clientName == null ) {
      buf.append( "*" );
    } else {
      buf.append( this.clientName );
    }
    buf.append( "_" );
    if ( this.logicalHost == null ) {
      buf.append( "*" );
    } else {
      buf.append( this.logicalHost );
    }
    return buf.toString();
  }
}
