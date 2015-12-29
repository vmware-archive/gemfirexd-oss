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

package com.gemstone.gemfire.compression;

/**
 * Interface for a compression codec.
 * 
 * @author rholmes
 */
public interface Compressor {

  /**
   * Compresses the input byte array.
   * 
   * @param input The data to be compressed.
   * 
   * @return A compressed version of the input parameter.
   * 
   * @throws CompressionException
   */
  public byte[] compress(byte[] input);
  
  /**
   * Decompresses a compressed byte array.
   * 
   * @param input A compressed byte array.
   * 
   * @return an uncompressed version of compressed input byte array data.
   * 
   * @throws CompressionException
   */
  public byte[] decompress(byte[] input);
}