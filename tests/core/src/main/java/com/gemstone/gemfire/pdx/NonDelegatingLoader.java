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
package com.gemstone.gemfire.pdx;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.shared.Version;

class NonDelegatingLoader extends ClassLoader {

  
  public NonDelegatingLoader(ClassLoader parent) {
    super(parent);
  }

  @Override
  public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if(!name.contains("SeparateClassloaderPdx")) {
      return super.loadClass(name, resolve);
    }
    URL url = super.getResource(name.replace('.', File.separatorChar) + ".class");
    if(url == null) {
      throw new ClassNotFoundException();
    }
    HeapDataOutputStream hoas = new HeapDataOutputStream(Version.CURRENT);
    InputStream classStream;
    try {
      classStream = url.openStream();
      while(true) {
        byte[] chunk = new byte[1024];
        int read = classStream.read(chunk);
        if(read < 0) {
          break;
        }
        hoas.write(chunk, 0, read);
      }
    } catch (IOException e) {
      throw new ClassNotFoundException("Error reading class", e);
    }
    
    Class clazz = defineClass(name, hoas.toByteBuffer(), null);
    if(resolve) {
      resolveClass(clazz);
    }
    return clazz;
  }
  
}