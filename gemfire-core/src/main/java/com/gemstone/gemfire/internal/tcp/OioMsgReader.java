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
package com.gemstone.gemfire.internal.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.shared.Version;

/**
 * A message reader which reads from the socket using
 * the old io.
 * @author dsmith
 *
 */
public class OioMsgReader extends MsgReader {

  public OioMsgReader(Connection conn, Version version) {
    super(conn, version);
  }

  @Override
  public ByteBuffer readAtLeast(int bytes) throws IOException {
    byte[] buffer = new byte[bytes];
    conn.readFully(conn.getSocket().getInputStream(), buffer, bytes);
    return ByteBuffer.wrap(buffer);
  }

}
