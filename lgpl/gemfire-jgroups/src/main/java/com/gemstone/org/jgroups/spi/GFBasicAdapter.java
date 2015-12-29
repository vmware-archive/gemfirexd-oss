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
package com.gemstone.org.jgroups.spi;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.SockCreator;
import com.gemstone.org.jgroups.util.VersionedStreamable;

public interface GFBasicAdapter {

  void invokeToData(VersionedStreamable obj,
      DataOutput out) throws IOException;

  void writeObject(Object obj, DataOutput out) throws IOException;

  void invokeFromData(VersionedStreamable obj,
      DataInput in) throws IOException, ClassNotFoundException;

  <T> T readObject(DataInput in) throws IOException, ClassNotFoundException;

  short getMulticastVersionOrdinal();

  short getSerializationVersionOrdinal(short version);

  short getCurrentVersionOrdinal();

  byte[] serializeWithVersion(Object obj, int destVersionOrdinal);

  void serializeJGMessage(Message message, DataOutputStream out) throws IOException;

  void deserializeJGMessage(Message message, DataInputStream in) throws IOException, IllegalAccessException, InstantiationException;

  ObjectOutput getObjectOutput(DataOutputStream out) throws IOException;

  ObjectInput getObjectInput(DataInputStream in) throws IOException;

  void writeString(String str, DataOutput out) throws IOException;

  String readString(DataInput in) throws IOException;

  void writeStringArray(String[] strings, DataOutput out) throws IOException;

  String[] readStringArray(DataInput in) throws IOException;

  DataOutputStream getVersionedDataOutputStream(DataOutputStream dos, short version) throws IOException;

  DataInputStream getVersionedDataInputStream(DataInputStream instream,
      short version) throws IOException;

  byte[] readByteArray(DataInput in) throws IOException;

  void writeByteArray(byte[] array, DataOutput out) throws IOException;

  void writeProperties(Properties props, DataOutput oos) throws IOException;

  Properties readProperties(DataInput in) throws IOException, ClassNotFoundException;

  int getGossipVersionForOrdinal(short serverOrdinal);

  boolean isVersionForStreamAtLeast(DataOutput stream, short version);

  boolean isVersionForStreamAtLeast(DataInput stream, short version);

  String getHostName(InetAddress ip_addr);

  RuntimeException getAuthenticationFailedException(String failReason);

  SockCreator getSockCreator();


  RuntimeException getSystemConnectException(String localizedString);

  Object getForcedDisconnectException(String localizedString);

  RuntimeException getDisconnectException(String localizedString);

  RuntimeException getGemFireConfigException(String string);

  void setDefaultGemFireAttributes(IpAddress local_addr);

  void setGemFireAttributes(IpAddress addr, Object attr);

  InetAddress getLocalHost() throws UnknownHostException;

  void checkDisableDNS();

  String getVmKindString(int vmKind);
}
