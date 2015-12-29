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
package com.gemstone.org.jgroups.stack;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;

import com.gemstone.org.jgroups.util.ConnectionWatcher;
import com.gemstone.org.jgroups.util.GFLogWriter;
import com.gemstone.org.jgroups.util.SockCreator;

public class SockCreatorImpl implements SockCreator {

  @Override
  public boolean useSSL() {
    return false;
  }

  @Override
  public Socket connect(InetAddress ipAddress, int port, GFLogWriter log,
      int connectTimeout, ConnectionWatcher watcher, boolean clientToServer,
      int bufferSize, boolean useSSL_ignored) throws IOException {
    Socket socket = new Socket();
    SocketAddress addr = new InetSocketAddress(ipAddress, port);
    if (connectTimeout > 0) {
      socket.connect(addr, connectTimeout);
    } else {
      socket.connect(addr);
    }
    return socket;
  }

  @Override
  public Socket connect(InetAddress ipAddress, int port, GFLogWriter log, int i,
      ConnectionWatcher watcher, boolean clientToServer) throws IOException {
    Socket socket = new Socket();
    SocketAddress addr = new InetSocketAddress(ipAddress, port);
    socket.connect(addr);
    return socket;
  }
}
