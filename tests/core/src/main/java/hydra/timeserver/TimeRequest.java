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
package hydra.timeserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A request to the time server.
 * @author dsmith
 *
 */
class TimeRequest {
  private final byte packetId;
  private final boolean isNanoTime;

  public TimeRequest(final byte packetId, final boolean isNanoTime) {
    this.packetId = packetId;
    this.isNanoTime = isNanoTime;
  }

  public byte getPacketId() {
    return packetId;
  }

  public boolean isNanoTime()
  {
    return isNanoTime;
  }

  public byte[] toByteArray()
  {
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(ba);
    try {
      dataOut.writeByte(packetId);
      dataOut.writeBoolean(isNanoTime);
      dataOut.close();
    } catch (IOException e) {
      throw new RuntimeException("ByteArrayOutputStream will not throw an IOException");
    }
    return ba.toByteArray();
  }

  public static TimeRequest fromByteArray(byte[] bytes) throws IOException
  {
    ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(bi);
    byte packetId = dataIn.readByte();
    boolean isNanoTime = dataIn.readBoolean();
    return new TimeRequest(packetId, isNanoTime);

  }

}
