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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;

/**
 * A response from the TCP server with information
 * about the server
 * @author dsmith
 * @since 5.7
 */
public class InfoResponse implements DataSerializable {
  private static final long serialVersionUID = 6249492407448855032L;
  private String[] info;
  public InfoResponse(String[] info) {
    this.info = info;
  }
  
  /** Used by DataSerializer */
  public InfoResponse() {
  }
  
  public String[] getInfo() {
    return info;
  }

  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    info = DataSerializer.readStringArray(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeStringArray(info, out);
  }
}
