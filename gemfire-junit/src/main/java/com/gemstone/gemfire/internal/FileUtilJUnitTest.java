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
package com.gemstone.gemfire.internal;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
public class FileUtilJUnitTest extends TestCase {
  
  public void testCopyFile() throws IOException {
    File source = File.createTempFile("FileUtilJUnitTest", null);
    File dest = File.createTempFile("FileUtilJUnitTest", null);
    try {
      FileOutputStream fos = new FileOutputStream(source);
      DataOutput daos = new DataOutputStream(fos);
      try {
        for(long i =0; i < FileUtil.MAX_TRANSFER_SIZE * 2.5 / 8; i++) {
          daos.writeLong(i);
        }
      } finally {
        fos.close();
      }
      FileUtil.copy(source, dest);

      FileInputStream fis = new FileInputStream(dest);
      DataInput dis = new DataInputStream(fis);
      try {
        for(long i =0; i < FileUtil.MAX_TRANSFER_SIZE * 2.5 / 8; i++) {
          assertEquals(i, dis.readLong());
        }
        assertEquals(-1, fis.read());
      } finally {
        fis.close();
      }
    } finally {
      source.delete();
      dest.delete();
    }
  }

}
