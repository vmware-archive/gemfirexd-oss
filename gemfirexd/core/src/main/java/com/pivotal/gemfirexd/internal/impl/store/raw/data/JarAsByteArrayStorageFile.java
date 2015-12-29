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
package com.pivotal.gemfirexd.internal.impl.store.raw.data;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.io.StorageFile;
import com.pivotal.gemfirexd.internal.io.StorageRandomAccessFile;

/**
 * 
 * @author kneeraj
 * 
 */
@SuppressWarnings("serial")
public class JarAsByteArrayStorageFile implements StorageFile, DataSerializable {

  private byte[] jarBytes;

  private String sqlName;

  public JarAsByteArrayStorageFile(String sql_Name) {
    //this.jarBytes = jar_bytes;
    this.sqlName = sql_Name;
  }

  public void init(InputStream source) throws StandardException {
    if (source != null) {
      byte[] data = new byte[4096];
      int len;
      //ByteOutputStream bos = new ByteOutputStream();
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      try {
        while ((len = source.read(data)) != -1) {
          bos.write(data, 0, len);
        }
        this.jarBytes = bos.toByteArray();
      } catch (IOException ioe) {
        throw StandardException.newException(
            SQLState.FILE_UNEXPECTED_EXCEPTION, ioe);
      }
    }
  }

  public String getSqlName() {
    return this.sqlName;
  }

  public byte[] getJarBytes() {
    return this.jarBytes;
  }

  @Override
  public String[] list() {
    return null;
  }

  @Override
  public boolean canWrite() {
    return false;
  }

  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public boolean isDirectory() {
    return false;
  }

  @Override
  public boolean delete() {
    return false;
  }

  @Override
  public boolean deleteAll() {
    return false;
  }

  @Override
  public String getPath() {
    return null;
  }

  @Override
  public String getCanonicalPath() throws IOException {
    return null;
  }

  @Override
  public String getName() {
    return this.sqlName;
  }

  @Override
  public URL getURL() throws MalformedURLException {
    return null;
  }

  @Override
  public boolean createNewFile() throws IOException {
    return false;
  }

  @Override
  public boolean renameTo(StorageFile newName) {
    return false;
  }

  @Override
  public boolean mkdir() {
    return false;
  }

  @Override
  public boolean mkdirs() {
    return false;
  }

  @Override
  public long length() {
    return this.jarBytes == null ? 0 : this.jarBytes.length;
  }

  @Override
  public StorageFile getParentDir() {
    return null;
  }

  @Override
  public boolean setReadOnly() {
    return false;
  }

  @Override
  public OutputStream getOutputStream() throws FileNotFoundException {
    return null;
  }

  @Override
  public OutputStream getOutputStream(boolean append)
      throws FileNotFoundException {
    return null;
  }

  @Override
  public InputStream getInputStream() throws FileNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(this.jarBytes);
    return bis;
  }

  @Override
  public int getExclusiveFileLock() {
    return StorageFile.NO_FILE_LOCK_SUPPORT;
  }

  @Override
  public void releaseExclusiveFileLock() {

  }

  @Override
  public StorageRandomAccessFile getRandomAccessFile(String mode)
      throws FileNotFoundException {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeUTF(this.sqlName);
    DataSerializer.writeByteArray(this.jarBytes, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.sqlName = in.readUTF();
    this.jarBytes = DataSerializer.readByteArray(in);
  }
}
