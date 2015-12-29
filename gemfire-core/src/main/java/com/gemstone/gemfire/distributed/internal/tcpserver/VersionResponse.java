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
package com.gemstone.gemfire.distributed.internal.tcpserver;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.internal.shared.Version;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Get GemFire version of the member running TcpServer.
 * @author shobhit
 * @since 7.1
 */
public class VersionResponse implements DataSerializable {


  private static final long serialVersionUID = 8320323031808601748L;
  private short versionOrdinal = Version.TOKEN.ordinal();

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeShort(versionOrdinal);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    versionOrdinal = in.readShort();
  }

  public short getVersionOrdinal() {
    return versionOrdinal;
  }

  public void setVersionOrdinal(short versionOrdinal) {
    this.versionOrdinal = versionOrdinal;
  }
}
