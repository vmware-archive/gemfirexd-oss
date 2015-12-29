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
package com.gemstone.gemfire.cache.hdfs.internal.hoplog;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;

import com.gemstone.gemfire.cache.hdfs.internal.org.apache.hadoop.io.SequenceFile;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.internal.cache.persistence.soplog.ComponentLogWriter;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.internal.util.LogService;

/**
 * Manages DDL statements in a hoplog file for GemFireStore.
 *  
 * @author hemantb
 */
public class DDLHoplogOrganizer {

  private HDFSStore hdfsstore;
  private ComponentLogWriter logger;
  static String TEMP_HOPLOG_EXTENSION = ".tmp"; 
  static String DDL_HOPLOG_EXTENSION = ".ddlhop";
  static String META_FOLDER_NAME = ".hopmeta";
  private final Path metaFolderPath;
  private final FileSystem fileSystem;
  private final Configuration conf;
  
  public DDLHoplogOrganizer(HDFSStore hdfsstore) {
    this.hdfsstore = hdfsstore;
    Path basePath = new Path(hdfsstore.getHomeDir());
    try {
      this.fileSystem = ((HDFSStoreImpl)hdfsstore).getFileSystem();
    } catch (IOException e) {
      throw new HDFSIOException(e.getMessage(), e);
    }
    this.metaFolderPath = this.fileSystem.makeQualified(new Path(basePath, META_FOLDER_NAME));
    this.conf = new Configuration(fileSystem.getConf());
    
    logger = ComponentLogWriter.getHoplogLogWriter("DDL" + this.hdfsstore.getName(), LogService.logger());
  }

  public void close() throws IOException {
    
  }
  
  public void flush(Iterator<byte[]> keyIter, Iterator<byte[]> valueIter) {
    flush(keyIter, valueIter, null);
  }
  /**
   * Flushes the DDLConflatable statements to HDFS in a new DDL 
   * file in the .meta folder
   * 
   * @param keyIter
   * @param valueIter
   * @param version - used only for testing. . Should be passed as null for other purposes. 
   * @throws IOException
   */
  public void flush(Iterator<byte[]> keyIter, Iterator<byte[]> valueIter, Version version) {
    assert keyIter != null;
    assert valueIter != null;
    
    logger.fine("Initializing DDL hoplog flush operation");
    Path filePath = null;
    try {
      // create a tmp DDL hoplog 
      filePath = getTmpDDLHoplogPath();
      if (logger.fineEnabled())
        logger.fine("Creating a temporary hoplog " + filePath.getName());
      SequenceFile.Writer writer = AbstractHoplog.getSequenceFileWriter(filePath, conf, logger, version);
      
      assert keyIter.hasNext() == valueIter.hasNext();
      
      // persist the DDl statements in the file 
      while (keyIter.hasNext() && valueIter.hasNext()) {
        
        writer.append(new BytesWritable(keyIter.next()), new BytesWritable(valueIter.next()));
        
        assert keyIter.hasNext() == valueIter.hasNext();
        
      }
      if (logger.fineEnabled())
        logger.fine("Closing hoplog " + filePath.getName());
      writer.close();
      // rename the hoplog to remove the tmp extension. 
      Path newestFile = makeLegitimate(filePath);
      
      deleteOlderDDLHoplogs(newestFile);
            
    } catch (IOException e) {
      logger.warning(LocalizedStrings.HOPLOG_FLUSH_OPERATION_FAILED, e);
      throw new HDFSIOException(e.getMessage(), e);
    } 
  }
  
  /**
   * Deletes all the files other than the newest one. 
   * 
   * @throws IOException
   */
  private void deleteOlderDDLHoplogs(Path newestFile) throws IOException{
    // do not expect a null file here. 
    assert newestFile != null;
    
    FileStatus[] listOfStatus = null;
    try {
      listOfStatus = this.fileSystem.listStatus(metaFolderPath);
    } catch (FileNotFoundException e) {
      // do not expect a file not found exception 
      assert false;
    }
    
    if (listOfStatus == null) {
      if (logger.fineEnabled())
        logger.fine("No files found that were older than: " + newestFile.getName());
      return;
    }
    
    // delete all the files other than the newest file 
    for (FileStatus status : listOfStatus) {
      if (status.isDirectory())
        continue;
      if (!status.getPath().getName().equals(newestFile.getName())) {
        if (logger.fineEnabled())
          logger.fine("Deleting old hoplog " + status.getPath().getName());
        fileSystem.delete(status.getPath(), false);
        if (logger.fineEnabled())
          logger.fine("Deleted old hoplog " + status.getPath().getName());
      }
    }
    
  }
  
  /**
   * Gets the path of the newest DDL file in the .meta folder
   * 
   * @throws IOException
   */
  private FileStatus getPathOfNewestFile(FileStatus[] listOfStatus) throws IOException {
    long timestamp = Long.MIN_VALUE;
    FileStatus newerFile = null;
    
    for (FileStatus  status : listOfStatus) {
      String statusFileName = status.getPath().getName();

      // skip all files other than hoplog ones
      if (!statusFileName.endsWith(DDL_HOPLOG_EXTENSION)) {
        continue;
      }
      //if (statusFileName.endsWith(TEMP_HOPLOG_EXTENSION))
      //  continue;

      String fileName = statusFileName.substring(0, statusFileName.length()
          - DDL_HOPLOG_EXTENSION.length());

      long newTimeStamp = Long.parseLong(fileName);
      if (newTimeStamp > timestamp) {
        newerFile = status;
        timestamp = newTimeStamp;
      }
    }
    if (logger.fineEnabled())
      logger.fine("Newest hoplog " + newerFile.getPath().getName());
    
    return newerFile;
  }
  
  /**
   * get writer of a temporary sequence file 
   * @throws IOException
   */
  Path getTmpDDLHoplogPath() throws IOException {
    Path filePath = null;
    String name = System.currentTimeMillis() + DDL_HOPLOG_EXTENSION;
    filePath = new Path(metaFolderPath, name + TEMP_HOPLOG_EXTENSION );
    return filePath;
    
  }
  
  /**
   * renames a temporary hoplog file to a legitimate name.
   */
  Path makeLegitimate(Path filePath) throws IOException {
    String name = filePath.getName();
    assert name.endsWith(TEMP_HOPLOG_EXTENSION);

    int index = name.lastIndexOf(TEMP_HOPLOG_EXTENSION);
    name = name.substring(0, index);
    
    if (logger.fineEnabled())
      logger.fine("Renaming hoplog to " + name);
    Path parent = filePath.getParent();
    Path newPath = new Path(parent, name);
    fileSystem.rename(filePath, newPath);
    return newPath;
  }
 
  /**
   * Returns the current timestamp of the current hoplog 
   * Required for PXF service
   * 
   * @throws IOException
   */
  public long getCurrentHoplogTimeStamp() throws IOException {
    FileStatus[] listOfStatus = null;
    
    if (!this.fileSystem.exists(metaFolderPath)) {
      logger.fine("Cant find any meta files as the store path does not exist " + META_FOLDER_NAME);
      return -1;
    }
    
    listOfStatus = this.fileSystem.listStatus(metaFolderPath);
    
    if (listOfStatus == null)
      return -1;
    
    FileStatus newestFile = getPathOfNewestFile(listOfStatus);
    
    if (newestFile == null)
      return -1;
    if (logger.fineEnabled())
      logger.fine("Returning timestamp of hoplog " + newestFile.getPath().getName());
    String fileTimeStamp = newestFile.getPath().getName().substring(0, 
        newestFile.getPath().getName().length()- DDL_HOPLOG_EXTENSION.length());
    
    return Long.parseLong(fileTimeStamp);
  }
  public static class DDLHoplog {
    private ArrayList<byte[]> ddlstatements;
    private Version ddlFileVersion; 
    public DDLHoplog(ArrayList<byte[]> ddlstatements, Version ddlFileVersion){
      this.ddlstatements = ddlstatements;
      this.ddlFileVersion = ddlFileVersion;
    }
    public ArrayList<byte[]> getDDLStatements() {
      return ddlstatements;
    }
    public Version getDDLVersion() {
      return ddlFileVersion;
    }
  }
  /**
   * Returns all the DDL conflatable statements in the newest file
   * @throws IOException
   */
  public DDLHoplog getDDLStatementsForReplay() throws IOException {
    FileStatus[] listOfStatus = null;
    
    try {
      listOfStatus = this.fileSystem.listStatus(metaFolderPath);
    } catch (FileNotFoundException e) {
      return null;
    }
    
    if (listOfStatus == null)
      return null;
    
    FileStatus newestFile = getPathOfNewestFile(listOfStatus);
    
    if (newestFile == null)
      return null;
    
    if (logger.fineEnabled())
      logger.fine("Returning DDL statements of hoplog " + newestFile.getPath().getName());
    
    SequenceFileHoplog.SequenceFileIterator iter = new SequenceFileHoplog.SequenceFileIterator(this.fileSystem, 
        newestFile.getPath(), 0, Long.MAX_VALUE, conf, logger);
    Version v  = iter.getVersion();
    
    ArrayList<byte[]> listOfStatements = new ArrayList<byte[]>();
    while (iter.hasNext()) {
      iter.next();
      listOfStatements.add(iter.getValue());
    }
    
    
    return new DDLHoplog(listOfStatements, v);
    
  }
}
