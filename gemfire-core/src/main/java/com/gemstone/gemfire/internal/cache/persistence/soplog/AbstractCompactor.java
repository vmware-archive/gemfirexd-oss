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
package com.gemstone.gemfire.internal.cache.persistence.soplog;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogReader;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplog.SortedOplogWriter;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedOplogSetImpl.MergedIterator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.Metadata;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SerializedComparator;
import com.gemstone.gemfire.internal.cache.persistence.soplog.SortedReader.SortedIterator;
import com.gemstone.gemfire.internal.util.AbortableTaskService;
import com.gemstone.gemfire.internal.util.AbortableTaskService.AbortableTask;
import com.gemstone.gemfire.internal.util.LogService;

public abstract class AbstractCompactor<T extends Comparable<T>> implements Compactor {
  /** the logger */
  protected final ComponentLogWriter logger;
  
  /** the soplog factory */
  protected final SortedOplogFactory factory;
  
  /** the fileset */
  protected final Fileset<T> fileset;
  
  /** the soplog tracker */
  protected final CompactionTracker<T> tracker;

  /** thread for background compaction */
  protected final AbortableTaskService compactor;
  
  /** inactive files waiting to be deleted */
  private final Queue<TrackedReference<SortedOplogReader>> inactive;
  
  /** the soplogs */
  protected final List<Level> levels;
  
  /** provides consistent view of all levels */
  private final ReadWriteLock levelLock;

  /** test flag to abort compaction */
  volatile boolean testAbortDuringCompaction;
  
  /** test flag to delay compaction */
  volatile CountDownLatch testDelayDuringCompaction;
  
  public AbstractCompactor(SortedOplogFactory factory, 
      Fileset<T> fileset, CompactionTracker<T> tracker,
      Executor exec) {
    assert factory != null;
    assert fileset != null;
    assert tracker != null;
    assert exec != null;
    
    this.factory = factory;
    this.fileset = fileset;
    this.tracker = tracker;
    
    compactor = new AbortableTaskService(exec);
    inactive = new ConcurrentLinkedQueue<TrackedReference<SortedOplogReader>>();

    levelLock = new ReentrantReadWriteLock();
    levels = new ArrayList<Level>();
    
    logger = ComponentLogWriter.getSoplogLogWriter(factory.getConfiguration().getName(), LogService.logger());
  }
  
  @Override
  public final void add(SortedOplog soplog) throws IOException {
    levels.get(0).add(soplog);
  }
  
  @Override
  public final boolean compact() throws IOException {
    final CountDownLatch done = new CountDownLatch(1);
    final AtomicReference<Object> result = new AtomicReference<Object>(null);
    
    compact(true, new CompactionHandler() {
      @Override
      public void complete(boolean compacted) {
        result.set(compacted);
        done.countDown();
      }

      @Override
      public void failed(Throwable ex) {
        result.set(ex);
        done.countDown();
      }
    });
    
    try {
      done.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new InterruptedIOException();
    }
    
    Object val = result.get();
    if (val instanceof Throwable) {
      throw new IOException((Throwable) val);
    }
    
    assert val != null;
    return (Boolean) val;
  }
  
  @Override
  public final void compact(final boolean force, final CompactionHandler ch) {
    // TODO implement force=true, results in a single soplog
    AbortableTask task = new AbortableTask() {
      @Override
      public void runOrAbort(AtomicBoolean aborted) {
        if (logger.fineEnabled()) {
          logger.fine("Beginning compaction");
        }

        // TODO could do this in one go instead of level-by-level
        try {
          boolean compacted = false;
          for (Level level : levels) {
            if (aborted.get()) {
              if (logger.fineEnabled()) {
                logger.fine("Aborting compaction");
              }
              break;
            }
      
            checkTestDelay();
            if (force || level.needsCompaction()) {
              if (logger.fineEnabled()) {
                logger.fine("Compacting level " + level);
              }
              
              long start = factory.getConfiguration().getStatistics().getMinorCompaction().begin();
              try {
                compacted |= level.compact(aborted);
                factory.getConfiguration().getStatistics().getMinorCompaction().end(start);
                
              } catch (IOException e) {
                factory.getConfiguration().getStatistics().getMinorCompaction().error(start);
              }
            }
          }
          
          cleanupInactive();
          if (ch != null) {
            if (logger.fineEnabled()) {
              logger.fine("Completed compaction");
            }
            ch.complete(compacted);
          }
        } catch (Exception e) {
          if (logger.fineEnabled()) {
            logger.fine("Encountered an error during compaction", e);
          }
          if (ch != null) {
            ch.failed(e);
          }
        }
      }
      
      @Override
      public void abortBeforeRun() {
        if (ch != null) {
          ch.complete(false);
        }
      }
    };
    compactor.execute(task);
  }
  
  @Override
  public final CompactionTracker<?> getTracker() {
    return tracker;
  }

  @Override
  public final Fileset<?> getFileset() {
    return fileset;
  }
  
  @Override
  public final Collection<TrackedReference<SortedOplogReader>> getActiveReaders(
      byte[] start, byte[] end) {
    
    // need to coordinate with clear() so we can get a consistent snapshot
    // across levels
    levelLock.readLock().lock();
    try {
      // TODO this seems very garbage-y
      List<TrackedReference<SortedOplogReader>> soplogs = new ArrayList<TrackedReference<SortedOplogReader>>();
      for (Level level : levels) {
        soplogs.addAll(level.getSnapshot(start, end));
      }
      return soplogs;
    } finally {
      levelLock.readLock().unlock();
    }
  }
  
  @Override
  public final void clear() throws IOException {
    if (logger.fineEnabled()) {
      logger.fine("Clearing compactor");
    }
    
    compactor.abortAll();
    releaseTestDelay();
    compactor.waitForCompletion();

    levelLock.writeLock().lock();
    try {
      for (Level l : levels) {
        l.clear();
      }
    } finally {
      levelLock.writeLock().unlock();
    }
    
    cleanupInactive();
  }

  @Override
  public final void close() throws IOException {
    if (logger.fineEnabled()) {
      logger.fine("Closing compactor");
    }

    compactor.abortAll();
    releaseTestDelay();
    compactor.waitForCompletion();
    
    levelLock.writeLock().lock();
    try {
      for (Level l : levels) {
        l.close();
      }
    } finally {
      levelLock.writeLock().unlock();
    }
    
    TrackedReference<SortedOplogReader> tr;
    while ((tr = inactive.poll()) != null) {
      deleteInactive(tr);
    }
    inactive.clear();
  }
  
  /**
   * Creates a new soplog by merging the supplied soplog readers.
   * 
   * @param readers the readers to merge
   * @param collect true if deleted entries should be removed
   * @return the merged soplog
   * 
   * @throws IOException error during merge operation
   */
  protected SortedOplog merge(
      Collection<TrackedReference<SortedOplogReader>> readers, 
      boolean collect,
      AtomicBoolean aborted) throws IOException {
    
    SerializedComparator sc = null;
    List<SortedIterator<ByteBuffer>> iters = new ArrayList<SortedIterator<ByteBuffer>>();
    for (TrackedReference<SortedOplogReader> tr : readers) {
      iters.add(tr.get().scan());
      sc = tr.get().getComparator();
    }
    
    SortedIterator<ByteBuffer> scan = new MergedIterator(sc, readers, iters);
    try {
      if (!scan.hasNext()) {
        checkAbort(aborted);
        if (logger.fineEnabled()) {
          logger.fine(String.format("No entries left after compaction with readers %s ", readers));
        }
        return null;
      }

      File f = fileset.getNextFilename();
      if (logger.fineEnabled()) {
        logger.fine(String.format("Compacting soplogs %s into %s", readers, f));
      }

      if (testAbortDuringCompaction) {
        aborted.set(true);
      }

      SortedOplog soplog = factory.createSortedOplog(f);
      SortedOplogWriter wtr = soplog.createWriter();
      try {
        while (scan.hasNext()) {
          checkAbort(aborted);
          scan.next();
          if (!(collect && isDeleted(scan.value()))) {
            wtr.append(scan.key(), scan.value());
          }
        }

        EnumMap<Metadata, byte[]> metadata = mergeMetadata(readers);
        wtr.close(metadata);
        return soplog;
        
      } catch (IOException e) {
        wtr.closeAndDelete();
        throw e;
      }
    } finally {
      scan.close();
    }
  }
  
  protected EnumMap<Metadata, byte[]> mergeMetadata(
      Collection<TrackedReference<SortedOplogReader>> readers)
      throws IOException {
    // merge the metadata into the compacted file
    EnumMap<Metadata, byte[]> metadata = new EnumMap<Metadata, byte[]>(Metadata.class);
    for (Metadata meta : Metadata.values()) {
      byte[] val = null;
      for (TrackedReference<SortedOplogReader> tr : readers) {
        byte[] tmp = tr.get().getMetadata(meta);
        if (val == null) {
          val = tmp;
          
        } else if (tmp != null) {
          val = factory.getConfiguration().getMetadataCompactor(meta).compact(val, tmp);
        }
      }
      if (val != null) {
        metadata.put(meta, val);
      }
    }
    return metadata;
  }
  
  protected void releaseTestDelay() {
    if (testDelayDuringCompaction != null) {
      if (logger.fineEnabled()) {
        logger.fine("Releasing testDelayDuringCompaction");
      }
      testDelayDuringCompaction.countDown();
    }
  }

  protected void checkTestDelay() {
    if (testDelayDuringCompaction != null) {
      try {
        if (logger.fineEnabled()) {
          logger.fine("Waiting for testDelayDuringCompaction");
        }
        testDelayDuringCompaction.await();
        
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  
  /**
   * Returns the number of inactive readers.
   * @return the inactive readers
   */
  protected int countInactiveReaders() {
    return inactive.size();
  }
  
  /**
   * Returns the requested level for testing purposes.
   * @param level the level ordinal
   * @return the level
   */
  protected Level getLevel(int level) {
    return levels.get(level);
  }

  protected void cleanupInactive() throws IOException {
    for (Iterator<TrackedReference<SortedOplogReader>> iter = inactive.iterator(); iter.hasNext(); ) {
      TrackedReference<SortedOplogReader> tr = iter.next();
      if (!tr.inUse() && inactive.remove(tr)) {
        deleteInactive(tr);
      }
    }
  }

  protected void markAsInactive(Iterable<TrackedReference<SortedOplogReader>> snapshot, T attach) throws IOException {
    for (Iterator<TrackedReference<SortedOplogReader>> iter = snapshot.iterator(); iter.hasNext(); ) {
      TrackedReference<SortedOplogReader> tr = iter.next();
      if (logger.fineEnabled()) {
        logger.fine(String.format("Marking %s as inactive", tr));
      }
      
      inactive.add(tr);
      tracker.fileRemoved(tr.get().getFile(), attach);
      
      factory.getConfiguration().getStatistics().incActiveFiles(-1);
      factory.getConfiguration().getStatistics().incInactiveFiles(1);
    }
  }

  private boolean isDeleted(ByteBuffer value) {
    //first byte determines the value type
    byte valType = value.get(value.position());
    return SoplogToken.isTombstone(valType) || SoplogToken.isRemovedPhase2(valType);
  }
  
  private void checkAbort(AtomicBoolean aborted)
      throws InterruptedIOException {
    if (aborted.get()) {
      throw new InterruptedIOException();
    }
  }

  private void deleteInactive(TrackedReference<SortedOplogReader> tr)
      throws IOException {
    tr.get().close();
    if (tr.get().getFile().delete()) {
      if (logger.fineEnabled()) {
        logger.fine("Deleted inactive soplog " + tr.get().getFile());
      }
      
      tracker.fileDeleted(tr.get().getFile());
      factory.getConfiguration().getStatistics().incInactiveFiles(-1);
    }
  }

  /**
   * Organizes a set of soplogs for a given level.
   */
  protected static abstract class Level {
    /** the level ordinal position */
    protected final int level;
    
    public Level(int level) {
      this.level = level;
    }
    
    @Override
    public String toString() {
      return String.valueOf(level);
    }
    
    /**
     * Returns true if the level needs compaction.
     * @return true if compaction is needed
     */
    protected abstract boolean needsCompaction();

    /**
     * Obtains the current set of active soplogs for this level.
     * @return the soplog snapshot
     */
    protected List<TrackedReference<SortedOplogReader>> getSnapshot() {
      return getSnapshot(null, null);
    }

    /**
     * Obtains the current set of active soplogs for this level, optionally 
     * bounded by the start and end keys.
     * 
     * @param start the start key
     * @param end the end key
     * @return the soplog snapshot
     */
    protected abstract List<TrackedReference<SortedOplogReader>> getSnapshot(byte[] start, byte[] end);
    
    /**
     * Clears the soplogs that match the metadata filter.
     * @throws IOException error during close
     */
    protected abstract void clear() throws IOException;
    
    /**
     * Closes the soplogs managed by this level.
     * @throws IOException error closing soplogs
     */
    protected abstract void close() throws IOException;
    
    /**
     * Adds a new soplog to this level.
     * 
     * @param soplog the soplog
     * @throws IOException error creating reader
     */
    protected abstract void add(SortedOplog soplog) throws IOException;
    
    /**
     * Merges the current soplogs into a new soplog and promotes it to the next
     * level.  The previous soplogs are marked for deletion.
     * 
     * @param aborted true if the compaction should be aborted
     * @throws IOException error unable to perform compaction
     */
    protected abstract boolean compact(AtomicBoolean aborted) throws IOException;    
  }
}
