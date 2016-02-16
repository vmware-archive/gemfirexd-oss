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
package com.gemstone.gemfire.codeAnalysis.decode;
import java.io.*;

import com.gemstone.gemfire.codeAnalysis.decode.cp.CpUtf8;


public class CompiledAttribute {
    int attribute_name_index;  // java predefines some but others are allowed, too
    long attribute_length;
    byte info[];
    String name;

    CompiledAttribute( DataInputStream source ) throws IOException {
        int idx;

        attribute_name_index = source.readUnsignedShort();
        attribute_length = source.readInt();
        info = new byte[(int)attribute_length];
        for (idx=0; idx<attribute_length; idx++) {
            info[idx] = (byte)source.readUnsignedByte();
        }
    }
    
    public String name(CompiledClass dclass) {
      if (name == null) {
        name = ((CpUtf8)dclass.constant_pool[attribute_name_index]).stringValue();
      }
      return name;
    }
}