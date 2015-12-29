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
package com.gemstone.gemfire.internal.datasource;

/**
 * This class wraps the client connection factory and the corresponding
 * connection manager Object.
 * 
 * @author rreja
 */
public class ClientConnectionFactoryWrapper  {

  private Object clientConnFac;
  private Object manager;

  /**
   * Constructor.
   */
  public ClientConnectionFactoryWrapper(Object connFac, Object man) {
    this.clientConnFac = connFac;
    this.manager = man;
  }

  public void clearUp() {
    if (manager instanceof JCAConnectionManagerImpl) {
      ((JCAConnectionManagerImpl) this.manager).clearUp();
    }
    else if (manager instanceof FacetsJCAConnectionManagerImpl) {
      ((FacetsJCAConnectionManagerImpl) this.manager).clearUp();
    }
  }

  public Object getClientConnFactory() {
    return clientConnFac;
  }

  public Object getConnectionManager() {
    return this.manager;
  }
}
