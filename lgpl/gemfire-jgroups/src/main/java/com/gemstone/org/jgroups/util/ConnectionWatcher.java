/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */
package com.gemstone.org.jgroups.util;

import java.net.Socket;

/**
 * ConnectionWatcher is used to observe tcp/ip connection formation in SockCreator
 * implementations.
 * 
 * @author bschuchardt
 *
 */
public interface ConnectionWatcher {
  /**
   * this is invoked with the connecting socket just prior to issuing
   * a connect() call.  It can be used to start another thread or task
   * to monitor the connection attempt.  
   */
  public void beforeConnect(Socket socket);
  
  /**
   * this is invoked after the connection attempt has finished.  It can
   * be used to cancel the task started by beforeConnect
   */
  public void afterConnect(Socket socket);
}
