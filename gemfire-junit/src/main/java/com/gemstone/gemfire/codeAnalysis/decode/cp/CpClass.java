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


public class CpClass extends Cp {
    int name_index;
    private String name;

    CpClass( DataInputStream source ) throws IOException {
        name_index = source.readUnsignedShort();
    }
    /**
     * find and form the class name - remembering that '[' chars may come before
     * the class name to denote arrays of the given class.  Max of 255 array specs
     */
    public String className(CompiledClass info) {
        if (name == null)
            name = decodeNameRef(info);
        return name;
    }
    private String decodeNameRef(CompiledClass info) {
        return ((CpUtf8)info.constant_pool[name_index]).stringValue();
    }
}
