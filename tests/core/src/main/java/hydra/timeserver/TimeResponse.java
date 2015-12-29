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
 * A response from the time server.
 * @author dsmith
 *
 */
class TimeResponse {
  private final byte packetId;
  private final long timeStamp;

  public TimeResponse(final byte packetId, final long timeStamp) {
    this.packetId = packetId;
    this.timeStamp = timeStamp;
  }

  public byte getPacketId() {
    return packetId;
  }
  public long getTimeStamp() {
    return timeStamp;
  }

  public byte[] toByteArray()
  {
    ByteArrayOutputStream ba = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(ba);
    try {
      dataOut.writeByte(packetId);
      dataOut.writeLong(timeStamp);
      dataOut.close();
    } catch (IOException e) {
      throw new RuntimeException("ByteArrayOutputStream will not throw an IOException");
    }
    return ba.toByteArray();
  }

  public static TimeResponse fromByteArray(byte[] bytes) throws IOException
  {
    ByteArrayInputStream bi = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(bi);
    byte packetId = dataIn.readByte();
    long timeStamp = dataIn.readLong();
    return new TimeResponse(packetId, timeStamp);

  }

}
