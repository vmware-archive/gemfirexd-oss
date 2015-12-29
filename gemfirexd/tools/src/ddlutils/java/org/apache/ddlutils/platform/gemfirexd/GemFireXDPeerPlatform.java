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

package org.apache.ddlutils.platform.gemfirexd;

/**
 * Any extensions required for the embedded driver that creates a GemFireXD peer.
 * 
 * @author swale
 */
public class GemFireXDPeerPlatform extends GemFireXDPlatform {

  /** Database name of this platform. */
  public static final String DATABASENAME = "GemFireXDPeer";

  /** The GemFireXD jdbc driver for use as an embedded database. */
  public static final String JDBC_PEER_DRIVER =
      "com.pivotal.gemfirexd.jdbc.EmbeddedDriver";

  /**
   * Creates a new GemFireXD platform instance.
   */
  public GemFireXDPeerPlatform() {
    super();
    // supports getting identity values for batch statements using JDBC
    // Statement API
    getPlatformInfo().setIdentityValueReadableInBatchUsingStatement(true);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DATABASENAME;
  }

  @Override
  protected String getDriver() {
    return JDBC_PEER_DRIVER;
  }
}
