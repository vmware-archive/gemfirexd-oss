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

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

/** This interface does not define how to create socks
 * but defines a factory for creating sockets.
 * One of its implementations had already used
 * the name "SocketCreator" at the time the interface
 * was created.
 */
public interface SockCreator {

  boolean useSSL();

  Socket connect(InetAddress ipAddress, int port, GFLogWriter log,
      int connectTimeout, ConnectionWatcher watcher, boolean clientToServer,
      int i, boolean useSSL) throws IOException;

  Socket connect(InetAddress ipAddress, int port, GFLogWriter log, int i,
      ConnectionWatcher watcher, boolean clientToServer) throws IOException;
}
