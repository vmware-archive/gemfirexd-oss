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
package newWan.serial.filters;

import hydra.Log;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;


import com.gemstone.gemfire.cache.wan.GatewayTransportFilter;

public class MyTransportFilter1 implements GatewayTransportFilter {

  private String id = new String("MyTransportFilter1");
  
  Adler32 checker = new Adler32();

  public InputStream getInputStream(InputStream stream) {
    Log.getLogWriter().info("MyTransportFilter1: Decompressing stream " + stream.toString());
    return new CheckedInputStream(stream, checker);
  }

  public OutputStream getOutputStream(OutputStream stream) {
    Log.getLogWriter().info("MyTransportFilter1: Compressing stream " + stream.toString());
    return new CheckedOutputStream(stream, checker);
  }

  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public String toString() {
    return id;
  }
}
