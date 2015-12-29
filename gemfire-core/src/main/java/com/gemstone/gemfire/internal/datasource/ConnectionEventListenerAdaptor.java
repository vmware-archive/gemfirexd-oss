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
 * @author mitulb
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
public class ConnectionEventListenerAdaptor implements 
    javax.resource.spi.ConnectionEventListener,
    javax.sql.ConnectionEventListener {

  /**
   * @see javax.resource.spi.ConnectionEventListener#connectionClosed(javax.resource.spi.ConnectionEvent)
   */
  public void connectionClosed(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#localTransactionStarted(javax.resource.spi.ConnectionEvent)
   */
  public void localTransactionStarted(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#localTransactionCommitted(javax.resource.spi.ConnectionEvent)
   */
  public void localTransactionCommitted(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#localTransactionRolledback(javax.resource.spi.ConnectionEvent)
   */
  public void localTransactionRolledback(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * @see javax.resource.spi.ConnectionEventListener#connectionErrorOccurred(javax.resource.spi.ConnectionEvent)
   */
  public void connectionErrorOccurred(javax.resource.spi.ConnectionEvent arg0) {
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection close event.
   * 
   * @param event Connection event object
   */
  public void connectionClosed(javax.sql.ConnectionEvent event) {
  }

  /**
   * Implementation of call back function from ConnectionEventListener
   * interface. This callback will be invoked on connection error event.
   * 
   * @param event Connection event object
   */
  public void connectionErrorOccurred(javax.sql.ConnectionEvent event) {
  }
}
