/*

   Derby - Class com.pivotal.gemfirexd.internal.impl.io.DirFile

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

package com.pivotal.gemfirexd.internal.impl.io;


import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.io.StorageFile;
import com.pivotal.gemfirexd.internal.io.StorageRandomAccessFile;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;

/**
 * This class provides a disk based implementation of the StorageFile interface. It is used by the
 * database engine to access persistent data and transaction logs under the directory (default) subsubprotocol.
 */
// GemStone changes BEGIN
public // make this public so LOBStreamControl can access it
// GemStone changes END
class DirFile extends File implements StorageFile
//GemStone changes BEGIN
, PrivilegedExceptionAction<Object>
//GemStone changes END
{

//GemStone changes BEGIN
  private static final long serialVersionUID = -99617810905921770L;
//GemStone changes END

    /**
     * Construct a DirFile from a path name.
     *
     * @param path The path name.
     */
    public DirFile( String path)
    {
        super( path);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
  // GemStone changes BEGIN
  public // make this public so LOBStreamControl can access it
  // GemStone changes END
  DirFile( String directoryName, String fileName)
    {
        super( directoryName, fileName);
    }

    /**
     * Construct a DirFile from a directory name and a file name.
     *
     * @param directoryName The directory part of the path name.
     * @param fileName The name of the file within the directory.
     */
    DirFile( DirFile directoryName, String fileName)
    {
        super( (File) directoryName, fileName);
    }

    /**
     * Get the name of the parent directory if this name includes a parent.
     *
     * @return An StorageFile denoting the parent directory of this StorageFile, if it has a parent, null if
     *         it does not have a parent.
     */
    public StorageFile getParentDir()
    {
        String parent = getParent();
        if( parent == null)
            return null;
        return new DirFile( parent);
    }
    
    /**
     * Get the name of the directory of temporary files.
     *
     * @return The abstract name of the temp directory;
     */
    static StorageFile getTempDir() throws IOException
    {
        File temp = File.createTempFile("derby", "tmp");
        StorageFile parent = new DirFile( temp.getParent());
        temp.delete();

		return parent;
	} // End of getTempDir

    /**
     * Creates an output stream from a file name.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public FileOutputStream getOutputStream( ) throws FileNotFoundException
    {
        return new FileOutputStream( (File) this);
    }
    
    /**
     * Creates an output stream from a file name.
     *
     * @param append If true then data will be appended to the end of the file, if it already exists.
     *               If false and a normal file already exists with this name the file will first be truncated
     *               to zero length.
     *
     * @return an output stream suitable for writing to the file.
     *
     * @exception FileNotFoundException if the file exists but is a directory
     *            rather than a regular file, does not exist but cannot be created, or
     *            cannot be opened for any other reason.
     */
    public OutputStream getOutputStream( final boolean append) throws FileNotFoundException
    {
        return new FileOutputStream( getPath(), append);
    }

    /**
     * Creates an input stream from a file name.
     *
     * @return an input stream suitable for reading from the file.
     *
     * @exception FileNotFoundException if the file is not found.
     */
    public InputStream getInputStream( ) throws FileNotFoundException
    {
        return new FileInputStream( (File) this);
    }

    /**
     * Get an exclusive lock. This is used to ensure that two or more JVMs do not open the same database
     * at the same time.
     *
     *
     * @return EXCLUSIVE_FILE_LOCK_NOT_AVAILABLE if the lock cannot be acquired because it is already held.<br>
     *    EXCLUSIVE_FILE_LOCK if the lock was successfully acquired.<br>
     *    NO_FILE_LOCK_SUPPORT if the system does not support exclusive locks.<br>
     */
    public synchronized int getExclusiveFileLock()
	{
		if (exists())
		{
			delete();
		}
		try
        {
			//Just create an empty file
			RandomAccessFile lockFileOpen = new RandomAccessFile( (File) this, "rw");
			lockFileOpen.getFD().sync( );
			lockFileOpen.close();
		}catch(IOException ioe)
		{
			// do nothing - it may be read only medium, who knows what the
			// problem is
			if (SanityManager.DEBUG)
			{
				SanityManager.THROWASSERT(
                    "Unable to create Exclusive Lock File " + getPath(), ioe);
			}
		}
		
		return NO_FILE_LOCK_SUPPORT;
	} // end of getExclusiveFileLock

	/**
     * Release the resource associated with an earlier acquired exclusive lock
     *
     * @see #getExclusiveFileLock
     */
	public synchronized void releaseExclusiveFileLock()
	{
		if( exists())
		{
			delete(); 
		}
	} // End of releaseExclusiveFileLock

    /**
     * Get a random access (read/write) file.
     *
     * @param mode "r", "rw", "rws", or "rwd". The "rws" and "rwd" modes specify
     *             that the data is to be written to persistent store, consistent with the
     *             java.io.RandomAccessFile class ("synchronized" with the persistent
     *             storage, in the file system meaning of the word "synchronized").  However
     *             the implementation is not required to implement the "rws" or "rwd"
     *             modes. The implementation may treat "rws" and "rwd" as "rw". It is up to
     *             the user of this interface to call the StorageRandomAccessFile.sync
     *             method. If the "rws" or "rwd" modes are supported and the
     *             RandomAccessFile was opened in "rws" or "rwd" mode then the
     *             implementation of StorageRandomAccessFile.sync need not do anything.
     *
     * @return an object that can be used for random access to the file.
     *
     * @exception IllegalArgumentException if the mode argument is not equal to one of "r", "rw".
     * @exception FileNotFoundException if the file exists but is a directory rather than a regular
     *              file, or cannot be opened or created for any other reason .
     */
    public StorageRandomAccessFile getRandomAccessFile( String mode) throws FileNotFoundException
    {
        // Assume that modes "rws" and "rwd" are not supported.
        if( "rws".equals( mode) || "rwd".equals( mode))
            mode = "rw";
        return new DirRandomAccessFile( (File) this, mode);
    } // end of getRandomAccessFile

    /**
     * Rename the file denoted by this name. Note that StorageFile objects are immutable. This method
     * renames the underlying file, it does not change this StorageFile object. The StorageFile object denotes the
     * same name as before, however the exists() method will return false after the renameTo method
     * executes successfully.
     *
     *<p>It is not specified whether this method will succeed if a file already exists under the new name.
     *
     * @param newName the new name.
     *
     * @return <b>true</b> if the rename succeeded, <b>false</b> if not.
     */
    public boolean renameTo( StorageFile newName)
    {
        return super.renameTo( (File) newName);
    }

    /**
     * Deletes the named file and, if it is a directory, all the files and directories it contains.
     *
     * @return <b>true</b> if the named file or directory is successfully deleted, <b>false</b> if not
     */
    public boolean deleteAll()
    {
        if( !exists())
            return false;
        if( isDirectory())
        {
            String[] childList = super.list();
            String parentName = getPath();
            for( int i = 0; i < childList.length; i++)
            {
                if( childList[i].equals( ".") || childList[i].equals( ".."))
                    continue;
                DirFile child = new DirFile( parentName, childList[i]);
                if( ! child.deleteAll())
                    return false;
            }
        }
        return delete();
    } // end of deleteAll

	/**
	 * @see com.pivotal.gemfirexd.internal.io.StorageFile#getURL()
	 */
	public URL getURL() throws MalformedURLException {
		
		return toURL();
	}

  //GemStone changes BEGIN
  private long conglomId;
  private RandomAccessFile tmpFile;
  private FileChannel channel;
  
  private int capacity;
  private ByteBuffer reuseBuffer;
  private int readBytes; 
  private long fileOffset;
  
  private static final int oneMB = 0x8FFFFFF;
  
  private int action;
  private static final int CREATE_FILE = 1; 
  private static final int GET_FILE_CHANNEL = 2;
  private static final int GET_READ_BUFFER = 3;

  public final DirFile createTemporaryFile(final long conglomId) {
    this.conglomId = conglomId;
    this.action = CREATE_FILE;
    try {
      return (DirFile)java.security.AccessController.doPrivileged(this);
    } catch (PrivilegedActionException e) {
      throw GemFireXDRuntimeException.newRuntimeException("PrivilegedActionException", e.getException());
    }
  }
  
  public final FileChannel getChannel() {
    this.action = GET_FILE_CHANNEL;
    try {
      return (channel = (FileChannel)java.security.AccessController.doPrivileged(this));
    } catch (PrivilegedActionException e) {
      throw GemFireXDRuntimeException.newRuntimeException("PrivilegedActionException", e.getException());
    }
  }
  
  public final ByteBuffer getReadBuffer(int capacity, ByteBuffer reuseBuffer) {
    this.action = GET_READ_BUFFER;
    this.capacity = capacity;
    this.reuseBuffer = reuseBuffer;
    try {
      return (ByteBuffer) java.security.AccessController.doPrivileged(this);
    } catch (PrivilegedActionException e) {
      throw GemFireXDRuntimeException.newRuntimeException("PrivilegedActionException", e.getException());
    }
  }
  
  @Override
  public final Object run() throws StandardException {
    try {
      switch (action) {
        case CREATE_FILE:
          final DirFile df = new DirFile(this, "T" + this.conglomId + ".tmp");
          df.deleteOnExit();
          if(! df.createNewFile() ){
            throw StandardException
                .newException(SQLState.DATA_STORABLE_WRITE_EXCEPTION);
          }
          return df;
        case GET_FILE_CHANNEL:
          tmpFile = new RandomAccessFile(this, "rw");
          return tmpFile.getChannel();

        case GET_READ_BUFFER:
          if (capacity > oneMB) {
            final ByteBuffer b = channel.map(FileChannel.MapMode.READ_ONLY, 0,
                this.capacity);
            if (GemFireXDUtils.TraceTempFileIO) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_TEMP_FILE_IO,
                  "Mapped file=" + this + " bytes=" + b.limit());
            }
            readBytes = b.limit();
            return b;
          }
          else {
            reuseBuffer.clear();
            readBytes = channel.read(reuseBuffer);
            reuseBuffer.flip();
            assert reuseBuffer.limit() == readBytes: "limit="
                + reuseBuffer.limit() + " readBytes=" + readBytes;
            return reuseBuffer;
          }
          
        default:
          return null;
      }
    } catch (FileNotFoundException fnf) {
      throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,
          fnf);
    } catch (IOException ioe) {
      throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,
          ioe);
    }
  }
  
  public final int bytesRead() {
    return readBytes;
  }
	
  //GemStone changes END
}
