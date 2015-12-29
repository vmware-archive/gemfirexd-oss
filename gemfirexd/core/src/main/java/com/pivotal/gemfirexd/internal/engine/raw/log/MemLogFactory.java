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
package com.pivotal.gemfirexd.internal.engine.raw.log;

import java.io.File;
import java.util.Properties;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextManager;
import com.pivotal.gemfirexd.internal.iapi.services.daemon.Serviceable;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleControl;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.ModuleSupportable;
import com.pivotal.gemfirexd.internal.iapi.services.property.PersistentSet;
import com.pivotal.gemfirexd.internal.iapi.store.access.DatabaseInstant;
import com.pivotal.gemfirexd.internal.iapi.store.raw.RawStoreFactory;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ScanHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.data.DataFactory;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogFactory;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogScan;
import com.pivotal.gemfirexd.internal.iapi.store.raw.xact.TransactionFactory;
import com.pivotal.gemfirexd.internal.iapi.store.replication.master.MasterFactory;
import com.pivotal.gemfirexd.internal.io.StorageFile;

public class MemLogFactory implements LogFactory, ModuleControl,
    ModuleSupportable, Serviceable {
  public void abortLogBackup() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public boolean checkVersion(int requiredMajorVersion,
      int requiredMinorVersion, String feature) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public boolean checkpoint(RawStoreFactory rawStoreFactory,
      DataFactory dataFactory, TransactionFactory transactionFactory,
      boolean wait) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void checkpointInRFR(LogInstant cinstant, long redoLWM, long undoLWM,
      DataFactory df) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void deleteLogFileAfterCheckpointLogFile() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");

  }

  public void deleteOnlineArchivedLogFiles() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void disableLogArchiveMode() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void enableLogArchiveMode() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void endLogBackup(File toDir) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void flush(LogInstant where) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void freezePersistentStore() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public String getCanonicalLogPath() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public LogInstant getFirstUnflushedInstant() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public long getFirstUnflushedInstantAsLong() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public StorageFile getLogDirectory() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void getLogFactoryProperties(PersistentSet set)
      throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public MemLogger getLogger() {
    return new MemLogger();
  }

  public boolean inRFR() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public boolean inReplicationMasterMode() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public boolean isCheckpointInLastLogFile() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public boolean logArchived() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public ScanHandle openFlushedScan(DatabaseInstant startAt, int groupsIWant)
      throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public LogScan openForwardsFlushedScan(LogInstant startAt)
      throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public LogScan openForwardsScan(LogInstant startAt, LogInstant stopAt)
      throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void recover(RawStoreFactory rawStoreFactory, DataFactory dataFactory,
      TransactionFactory transactionFactory) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void setDatabaseEncrypted(boolean flushLog) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void startLogBackup(File toDir) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void startNewLogFile() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public void startReplicationMasterRole(MasterFactory masterFactory)
      throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");

  }

  public void stopReplicationMasterRole() {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");

  }

  public void unfreezePersistentStore() throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");
  }

  public StandardException markCorrupt(StandardException originalError) {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");

  }

  public int performWork(ContextManager context) throws StandardException {
    throw new UnsupportedOperationException(
        "This method is not supposed to be called!");

  }

  public boolean serviceASAP() {
    return false;
  }

  public boolean serviceImmediately() {
    return false;
  }

  public void boot(boolean create, Properties properties)
      throws StandardException {

  }

  public void stop() {

  }

  public boolean canSupport(String identifier, Properties properties) {

    return true;
  }

}
