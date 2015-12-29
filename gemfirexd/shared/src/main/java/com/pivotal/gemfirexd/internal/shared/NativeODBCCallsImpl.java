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

package com.pivotal.gemfirexd.internal.shared;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketImpl;
import java.util.Map;

import com.gemstone.gemfire.internal.shared.NativeCalls;
import com.gemstone.gemfire.internal.shared.NativeErrorException;
import com.gemstone.gemfire.internal.shared.OSType;
import com.gemstone.gemfire.internal.shared.TCPSocketOptions;

/**
 * Implementation of {@link NativeCalls} for ODBC driver. Everything is
 * delegated to native CNI calls.
 * 
 * @author swale
 * @since 7.5
 */
public final class NativeODBCCallsImpl extends NativeCalls {

  /**
   * {@inheritDoc}
   */
  @Override
  public native OSType getOSType();

  /**
   * {@inheritDoc}
   */
  @Override
  public native String getEnvironment(String name);

  /**
   * {@inheritDoc}
   */
  @Override
  public native void setEnvironment(String name, String value);

  /**
   * {@inheritDoc}
   */
  @Override
  public native int getProcessId();

  /**
   * {@inheritDoc}
   */
  @Override
  public native boolean isProcessActive(int processId);

  /**
   * {@inheritDoc}
   */
  @Override
  public native boolean killProcess(int processId);

  /**
   * {@inheritDoc}
   */
  @Override
  protected int getSocketKernelDescriptor(Socket sock, InputStream sockStream)
      throws UnsupportedOperationException {
    Method m;
    Field f;
    Object obj;

    // use the SocketImpl route
    try {
      // for GCJ: getPlainSocketImpl().getNativeFD()
      try {
        m = getAnyMethod(sock.getClass(), "getPlainSocketImpl");
      } catch (Exception ex) {
        // package private Socket.getImpl() to get SocketImpl
        m = getAnyMethod(sock.getClass(), "getImpl");
      }
      if (m != null) {
        m.setAccessible(true);
        final SocketImpl sockImpl = (SocketImpl)m.invoke(sock);
        if (sockImpl != null) {
          // GCJ has different impl inside SocketImpl that holds the native FD
          try {
            // GCJ getNativeFD()
            try {
              m = getAnyMethod(sockImpl.getClass(), "getNativeFD");
              if (m != null) {
                m.setAccessible(true);
                obj = m.invoke(sockImpl);
                if (obj instanceof Integer) {
                  return ((Integer)obj).intValue();
                }
              }
            } catch (Exception ex) {
              // go on to the other route
            }
            // for GCJ: VMPlainSocketImpl.getState().getNativeFD()
            f = getAnyField(sockImpl.getClass(), "impl");
            if (f != null) {
              f.setAccessible(true);
              obj = f.get(sockImpl);
              if (obj != null) {
                m = getAnyMethod(obj.getClass(), "getState");
                if (m != null) {
                  m.setAccessible(true);
                  obj = m.invoke(obj);
                  if (obj != null) {
                    m = getAnyMethod(obj.getClass(), "getNativeFD");
                    if (m != null) {
                      m.setAccessible(true);
                      obj = m.invoke(obj);
                      if (obj instanceof Integer) {
                        return ((Integer)obj).intValue();
                      }
                    }
                  }
                }
              }
            }
          } catch (NoSuchFieldException nfe) {
            // probably not GCJ
          } catch (NoSuchMethodException nme) {
            // probably not GCJ
          } catch (SecurityException se) {
            // try fallback
          }
        }
      }
    } catch (Exception ex) {
      // fallback to super's impl
    }
    return super.getSocketKernelDescriptor(sock, sockStream);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public native Map<TCPSocketOptions, Throwable> setSocketOptions(Socket sock,
      InputStream sockStream, Map<TCPSocketOptions, Object> optValueMap)
      throws UnsupportedOperationException;

  @Override
  protected native int getPlatformOption(TCPSocketOptions opt)
      throws UnsupportedOperationException;

  @Override
  protected native int setPlatformSocketOption(int sockfd, int level,
      int optName, TCPSocketOptions opt, Integer optVal, int optSize)
      throws UnsupportedOperationException, NativeErrorException;

  @Override
  protected native boolean isNoProtocolOptionCode(int errno)
      throws UnsupportedOperationException;
}
