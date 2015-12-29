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
package com.gemstone.gemfire.internal.compression;

import java.io.File;

import junit.framework.TestCase;

import org.junit.Test;

import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;

import org.xerial.snappy.SnappyUtils;

/**
 * Tests the Snappy {@link Compressor}.
 * @author rholmes
 */
public class SnappyCompressorJUnitTest extends TestCase {
  /**
   * @return true if our one and only codec (Snappy) is supported by the host platform running this test.
   */
  public static boolean isHostSupportedBySnappy() {
    // the return value of this method is only valid if library version is 1.0.4.1
    assertTrue(SnappyUtils.DISTRIBUTED_VERSION.contains(SnappyUtils.getNativeLibraryVersion()));
    return System.getProperty("os.name").contains("Windows")
        || System.getProperty("os.name").contains("Linux")
        || System.getProperty("os.name").contains("Mac");
  }
  
  public static boolean isHostSupportedByGemFire() {
    return System.getProperty("os.name").contains("Windows")
        || System.getProperty("os.name").contains("Linux")
        || System.getProperty("os.name").contains("Sun");
  }

  /**
   * Tests {@link Compressor#compress(byte[])} and {@link Compressor#decompress(byte[])} using
   * the Snappy compressor.
   */
  @Test
  public void testCompressByteArray() {
    if (isHostSupportedBySnappy()) {
      String compressMe = "Hello, how are you?";
      byte[] compressMeData = SnappyCompressor.getDefaultInstance().compress(compressMe.getBytes());
      String uncompressedMe = new String(SnappyCompressor.getDefaultInstance().decompress(compressMeData));

      assertEquals(compressMe, uncompressedMe);
    }
  }
  
  /**
   * Tests {@link SnappyCompressor()} constructor.
   */
  @Test
  public void testConstructor() {
    // intersection of host supported by Snappy and GemFire should use native library from product/lib
    if (isHostSupportedBySnappy() && isHostSupportedByGemFire()) {
      SnappyCompressor.getDefaultInstance();
      assertTrue(org.xerial.snappy.SnappyUtils.isNativeLibraryLoaded());
      // repeat findNativeLibrary and make sure it's pointing at a file NOT in tmpdir
      File nativeLibrary = org.xerial.snappy.SnappyUtils.findNativeLibrary();
      assertNotNull(nativeLibrary);
      assertTrue(nativeLibrary + " does not exist", nativeLibrary.exists());
      File tmpDir = new File(System.getProperty("java.io.tmpdir"));
      assertTrue(tmpDir.exists());
      File parent = nativeLibrary.getParentFile();
      assertNotNull(parent);
      assertFalse("Using native library from " + tmpDir + " instead of product/lib (this can occur in Eclipse if you have copied the native library to GEMFIRE_OUTPUT).", 
          tmpDir.equals(parent));
      
    // hosts supported by Snappy but not GemFire should extract native library into tmp dir
    } else if (isHostSupportedBySnappy()) {
      SnappyCompressor.getDefaultInstance();
      assertTrue(org.xerial.snappy.SnappyUtils.isNativeLibraryLoaded());
      // repeat findNativeLibrary and make sure it's pointing at a file in tmpdir
      File nativeLibrary = org.xerial.snappy.SnappyUtils.findNativeLibrary();
      assertNotNull(nativeLibrary);
      assertTrue(nativeLibrary + " does not exist", nativeLibrary.exists());
      File tmpDir = new File(System.getProperty("java.io.tmpdir"));
      assertTrue(tmpDir.exists());
      File parent = nativeLibrary.getParentFile();
      assertNotNull(parent);
      assertEquals(tmpDir, parent);
      
    // platform not supported by current version of Snappy
    } else {
      boolean failed = false;
      try {
        SnappyCompressor.getDefaultInstance();
        failed = true;
      } catch (Throwable expected) {
      }
      
      if (failed) {
        fail("Should have received an exception");
      }
    }
  }
}