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
package com.gemstone.gemfire.internal.ra;

import javax.naming.NamingException;
import javax.naming.Reference;
import javax.resource.ResourceException;
import javax.resource.spi.ConnectionManager;
import javax.resource.spi.ManagedConnectionFactory;

import com.gemstone.gemfire.ra.GFConnectionFactory;
/**
 * 
 * @author asif
 *
 */
public class GFConnectionFactoryImpl implements GFConnectionFactory
{
  final private ConnectionManager cm;

  final private ManagedConnectionFactory mcf;

  private Reference ref;

  public GFConnectionFactoryImpl(ManagedConnectionFactory mcf) {
    this.cm = null;
    this.mcf = mcf;
  }

  public GFConnectionFactoryImpl(ConnectionManager cm,
      ManagedConnectionFactory mcf) {
    this.cm = cm;
    this.mcf = mcf;
  }

  public GFConnectionImpl getConnection() throws ResourceException
  {
    return (GFConnectionImpl)cm.allocateConnection(mcf, null);
  }

  public void setReference(Reference ref)
  {
    this.ref = ref;

  }

  public Reference getReference() throws NamingException
  {
    return this.ref;
  }

}
