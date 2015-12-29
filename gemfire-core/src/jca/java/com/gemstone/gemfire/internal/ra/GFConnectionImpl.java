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

import com.gemstone.gemfire.internal.ra.spi.JCAManagedConnection;
import com.gemstone.gemfire.ra.GFConnection;

/**
 * 
 * @author asif
 *
 */
public class GFConnectionImpl implements GFConnection
{
  private JCAManagedConnection mc;

  private Reference ref;

  public GFConnectionImpl(JCAManagedConnection mc) {
    this.mc = mc;
  }

  public void resetManagedConnection(JCAManagedConnection mc)
  {
    this.mc = mc;
  }

  public void close() throws ResourceException
  {
    // Check if the connection is associated with a JTA. If yes, then
    // we should throw an exception on close being invoked.
    if (this.mc != null) {
      this.mc.onClose(this);
    }
  }

  public void invalidate()
  {
    this.mc = null;
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
