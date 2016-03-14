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
package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

import com.gemstone.gemfire.codeAnalysis.decode.CompiledClass;


public class CpFieldref extends Cp {
    int class_index;
    int name_and_type_index;
    CpFieldref( DataInputStream source ) throws IOException {
        class_index = source.readUnsignedShort();
        name_and_type_index = source.readUnsignedShort();
    }
    public String returnType(CompiledClass info) {
        return "not yet implemented";
    }
}