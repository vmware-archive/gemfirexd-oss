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
package com.gemstone.gemfire.cache.util;

/**
 * An <code>EndpointDoesNotExistException</code> indicates a client
 * <code>Endpoint</code> does not exist for the input name, host and
 * port.
 *
 * @author Barry Oglesby
 *
 * @since 5.0.2
 * @deprecated as of 5.7 use {@link com.gemstone.gemfire.cache.client pools} instead.
 */
@Deprecated
public class EndpointDoesNotExistException extends EndpointException {
private static final long serialVersionUID = 1654241470788247283L;

  /**
   * Constructs a new <code>EndpointDoesNotExistException</code>.
   * 
   * @param name The name of the requested <code>Endpoint</code>
   * @param host The host of the requested <code>Endpoint</code>
   * @param port The port of the requested <code>Endpoint</code>
   */
  public EndpointDoesNotExistException(String name, String host, int port) {
    super(name+"->"+host+":"+port);
  }
}
