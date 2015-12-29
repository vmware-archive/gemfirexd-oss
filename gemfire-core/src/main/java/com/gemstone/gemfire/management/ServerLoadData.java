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
package com.gemstone.gemfire.management;

import java.beans.ConstructorProperties;

/**
 * Composite data type used to distribute server load information.
 * 
 * @author rishim
 * @since 7.0
 */
public class ServerLoadData {

  private float connectionLoad;
  private float subscriberLoad;
  private float loadPerConnection;
  private float loadPerSubscriber;

  @ConstructorProperties( { "connectionLoad", "subscriberLoad",
      "loadPerConnection", "loadPerSubscriber" })
  public ServerLoadData(float connectionLoad, float subscriberLoad,
      float loadPerConnection, float loadPerSubscriber) {
    this.connectionLoad = connectionLoad;
    this.subscriberLoad = subscriberLoad;
    this.loadPerConnection = loadPerConnection;
    this.loadPerSubscriber = loadPerSubscriber;

  }

  /**
   * Returns the load on the server due to client to server connections.
   */
  public float getConnectionLoad() {
    return connectionLoad;
  }

  /**
   * Returns the load on the server due to subscription connections.
   */
  public float getSubscriberLoad() {
    return subscriberLoad;
  }

  /**
   * Returns an estimate of how much load each new connection will add to this
   * server. The Locator use this information to estimate the load on the server
   * before it receives a new load snapshot.
   */
  public float getLoadPerConnection() {
    return loadPerConnection;
  }

  /**
   * Returns an estimate of the much load each new subscriber will add to this
   * server. The Locator uses this information to estimate the load on the
   * server before it receives a new load snapshot.
   */
  public float getLoadPerSubscriber() {
    return loadPerSubscriber;
  }

  @Override
  public String toString() {

    return "{ServerLoad is : connectionLoad = " + connectionLoad
        + " subscriberLoad = " + subscriberLoad + " loadPerConnection = "
        + loadPerConnection + " loadPerSubscriber = " + loadPerSubscriber
        + " }";
  }

}
