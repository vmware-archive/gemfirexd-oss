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

package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.util.*;
import java.io.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.FilterRoutingInfo.FilterInfo;
import com.gemstone.gemfire.internal.cache.partitioned.PRLocallyDestroyedException;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.distributed.internal.*;
import java.util.concurrent.RejectedExecutionException;

/**
 * 
 * @author Eric Zoerner
 */
public class DestroyRegionOperation extends DistributedCacheOperation {

  @Override
  public boolean supportsDirectAck()
  {
    // Part of fix for bug 34450
    return false; // Changed to prevent problems with executing
                  // basicDestroyRegion in the waiting pool.
  }
  
  @Override
  protected boolean supportsAdjunctMessaging() {
    return false;
  }

  @Override
  public boolean canBeSentDuringShutdown() {
    return true;
  }

  private final boolean notifyOfRegionDeparture;

  private static final ThreadLocal regionDepartureNotificationDisabled = new ThreadLocal();
  /**
   * This was added to fix bug 41111
   */
  public static boolean isRegionDepartureNotificationOk() {
    return regionDepartureNotificationDisabled.get() != Boolean.TRUE;
  }

  /** Creates new instance of DestroyRegionOperation
   * @param notifyOfRegionDeparture was added to fix bug 41111. If false then don't
   * deliver afterRemoteRegionDeparture events.
   */
  public DestroyRegionOperation(RegionEventImpl event, boolean notifyOfRegionDeparture) {
    super(event);
    this.notifyOfRegionDeparture = notifyOfRegionDeparture;
  }

  @Override
  protected Set getRecipients()
  {
    CacheDistributionAdvisor advisor = getRegion().getCacheDistributionAdvisor();
    return advisor.adviseDestroyRegion();
  }

  @Override
  protected boolean shouldAck()
  {
    return true;
  }

  @Override
  protected CacheOperationMessage createMessage()
  {
    DestroyRegionMessage mssg;
    if (this.event instanceof BridgeRegionEventImpl) {
      mssg = new DestroyRegionWithContextMessage();
      ((DestroyRegionWithContextMessage)mssg).context = ((BridgeRegionEventImpl)this.event)
          .getContext();
    }
    else {
      mssg = new DestroyRegionMessage();
    }

    mssg.notifyOfRegionDeparture = this.notifyOfRegionDeparture;
    DistributedRegion rgn = getRegion();
    mssg.serialNum = rgn.getSerialNumber();
    Assert.assertTrue(mssg.serialNum != DistributionAdvisor.ILLEGAL_SERIAL);
    
    mssg.subregionSerialNumbers = rgn.getDestroyedSubregionSerialNumbers();
    
    RegionEventImpl rei = (RegionEventImpl)this.event;
    mssg.eventID = rei.getEventId();
    return mssg;
  }

  public static class DestroyRegionMessage extends CacheOperationMessage
   {
    protected EventID eventID;
    
    /** serial number of the region to be destroyed */
    protected int serialNum;
    
    /** map of subregion full paths to serial numbers */
    protected HashMap subregionSerialNumbers;

    protected boolean notifyOfRegionDeparture;
    /**
     * true if need to automatically recreate region, and mark destruction as a
     * reinitialization
     */
    protected transient LocalRegion lockRoot = null; // used for early destroy
                                                   // lock acquisition

    @Override
    protected InternalCacheEvent createEvent(DistributedRegion rgn)
        throws EntryNotFoundException
    {
      RegionEventImpl event = createRegionEvent(rgn);
      if (this.filterRouting != null) {
        event.setLocalFilterInfo(this.filterRouting
            .getFilterInfo(rgn.getMyId()));
      }
      event.setEventID(this.eventID);
      return event;
    }
    
    protected RegionEventImpl createRegionEvent(DistributedRegion rgn)
    {     
      RegionEventImpl event = new RegionEventImpl(rgn, getOperation(),
          this.callbackArg, true /* originRemote */, getSender());
      return event;
    }

    private Runnable destroyOp(final DistributionManager dm,
        final LocalRegion lclRgn, final boolean sendReply) {
      return new Runnable() {
        public void run()
        {
          final int oldLevel = LocalRegion
              .setThreadInitLevelRequirement(LocalRegion.BEFORE_INITIAL_IMAGE);
          // do this before CacheFactory.getInstance for bug 33471

          final LogWriterI18n logger = dm.getLoggerI18n();
          Throwable thr = null;
          try {
            if (lclRgn == null) {
              // following block is specific to buckets...
              // need to wait for queued bucket profiles to be processed
              // or this destroy may do nothing and result in a stale profile
              boolean waitForBucketInitializationToComplete = true;
              CacheDistributionAdvisee advisee = null;
              try {
                advisee = PartitionedRegionHelper.getProxyBucketRegion(GemFireCacheImpl.getInstance(), regionPath, 
                      waitForBucketInitializationToComplete);
              }
              catch (PRLocallyDestroyedException e) {
                // region not found - it's been destroyed
              }
              catch (RegionDestroyedException e) {
                // ditto
              }
              catch (PartitionedRegionException e) {
                if (e.getMessage().indexOf("destroyed") == -1) {
                  throw e;
                }
                // region failed registration & is unusable
              }

              if (advisee != null) {
                boolean isDestroy = op.isRegionDestroy() && !op.isClose();
                advisee.getDistributionAdvisor()
                    .removeIdWithSerial(getSender(), serialNum, isDestroy);
              }
              else
              if (logger.fineEnabled()) {
                logger.fine(this + " region not found, nothing to do");
              }
              return;
            } // lclRegion == null

            // refetch to use special destroy region logic
            final LocalRegion lr = getRegionFromPath(dm.getSystem(), lclRgn
                .getFullPath());
            if (lr == null) {
              if (logger.fineEnabled())
                logger.fine(this + " region not found, nothing to do");
              return;
            }
            // In some subclasses, lclRgn may be destroyed, so be careful not
            // to
            // allow a RegionDestroyedException to be thrown on lclRgn access
            if (!(lr instanceof DistributedRegion)) {
              if (logger.fineEnabled())
                logger.fine(this + " local scope region, nothing to do");
              return;
            }
            DistributedRegion rgn = (DistributedRegion)lr;

            InternalCacheEvent event = createEvent(rgn);
            if (DestroyRegionMessage.this.needsRouting && lclRgn.cache.getCacheServers().size() > 0) {
              lclRgn.generateLocalFilterRouting(event);
            }

            doRegionDestroy(event);
          }
          catch (RegionDestroyedException e) {
            logger.fine(this + "Region destroyed: nothing to do");
          }
          catch (CancelException e) {
            logger.fine(this + "Cancelled: nothing to do");
          }
          catch (EntryNotFoundException e) {
            logger.fine(this + "; Entry not found, nothing to do");
          }
          catch (Throwable t) {
            Error err;
            if (t instanceof Error && SystemFailure.isJVMFailureError(
                err = (Error)t)) {
              SystemFailure.initiateFailure(err);
              // If this ever returns, rethrow the error. We're poisoned
              // now, so don't let this thread continue.
              throw err;
            }
            // Whenever you catch Error or Throwable, you must also
            // check for fatal JVM error (see above).  However, there is
            // _still_ a possibility that you are dealing with a cascading
            // error condition, so you also need to check to see if the JVM
            // is still usable:
            SystemFailure.checkFailure();
            thr = t;
          }
          finally {
            LocalRegion.setThreadInitLevelRequirement(oldLevel);

            if (DestroyRegionMessage.this.lockRoot != null) {
              DestroyRegionMessage.this.lockRoot.releaseDestroyLock();
            }

            if (sendReply) {
              if (DestroyRegionMessage.this.processorId != 0) {
                ReplyException rex = null;
                if (thr != null) {
                  rex = new ReplyException(thr);
                }
                sendReply(getSender(), DestroyRegionMessage.this.processorId,
                    rex, getReplySender(dm));
              }
            }
            else if (thr != null) {
              logger.error(LocalizedStrings.DestroyRegionOperation_EXCEPTION_WHILE_PROCESSING__0_, this, thr);
            }
          }
        } // run
      };
    }

    @Override
    protected void basicProcess(final DistributionManager dm,
        final LocalRegion lclRgn) {

      Assert.assertTrue(this.serialNum != DistributionAdvisor.ILLEGAL_SERIAL);
      try {
        this.lockRoot = null;
        // may set lockRoot to the root region where destroyLock is acquired

        final boolean sendReply = true;

        // Part of fix for bug 34450 which was caused by a PR destroy region op
        // dead-locked with
        // a PR create region op. The create region op required an entry update
        // to release a
        // DLock needed by the PR destroy.. by moving the destroy to the waiting
        // pool, the entry
        // update is allowed to complete.
        dm.getWaitingThreadPool().execute(destroyOp(dm, lclRgn, sendReply));
      }
      catch (RejectedExecutionException e) {
        // rejected while trying to execute destroy thread
        // must be shutting down, just quit
      }
    }

    protected LocalRegion getRegionFromPath(InternalDistributedSystem sys,
        String path)
    {
      // allow a destroyed region to be returned if we're dealing with a
      // shared region, since another cache may
      // have already destroyed it in shared memory, in which our listeners
      // still need to be called and java region object cleaned up.
      GemFireCacheImpl c = (GemFireCacheImpl)CacheFactory.getInstance(sys);

      // only get the region while holding the appropriate destroy lock.
      // this prevents us from getting a "stale" region
      if (getOperation().isDistributed()) {
        String rootName = GemFireCacheImpl.parsePath(path)[0];
        this.lockRoot = (LocalRegion)c.getRegion(rootName);
        if (this.lockRoot == null)
          return null;
        this.lockRoot.acquireDestroyLock();
      }

      return (LocalRegion)c.getRegion(path);
    }

    private void disableRegionDepartureNotification() {
      if (!this.notifyOfRegionDeparture) {
        regionDepartureNotificationDisabled.set(Boolean.TRUE);
      }
    }
    private void enableRegionDepartureNotification() {
      if (!this.notifyOfRegionDeparture) {
        regionDepartureNotificationDisabled.remove();
      }
    }

    protected boolean doRegionDestroy(CacheEvent event)
        throws EntryNotFoundException
    {
      this.appliedOperation = true;
      RegionEventImpl ev = (RegionEventImpl)event;
      final DistributedRegion rgn = (DistributedRegion)ev.region;

      if (getOperation().isLocal()) {
        Assert.assertTrue(serialNum != DistributionAdvisor.ILLEGAL_SERIAL);
        disableRegionDepartureNotification();
        try {
          rgn.handleRemoteLocalRegionDestroyOrClose(
                                                    getSender(), serialNum, subregionSerialNumbers, !getOperation().isClose());
        } finally {
          enableRegionDepartureNotification();
        }
        return true;
      }

      LogWriterI18n logger = rgn.getCache().getLoggerI18n();


      try {
        String fullPath = null;
        if (logger.fineEnabled()) {
          fullPath = rgn.getFullPath();
          StringBuilder subregionNames = new StringBuilder();
          for (Iterator itr = rgn.debugGetSubregionNames().iterator(); itr
              .hasNext();) {
            subregionNames.append(itr.next());
            subregionNames.append(", ");
          }
          logger.fine("Processing DestroyRegionOperation, about to destroy "
              + fullPath + ", has immedidate subregions: "
              + subregionNames.toString());
        }
        if (getOperation() == Operation.REGION_LOAD_SNAPSHOT) {
          if (logger.fineEnabled()) {
            logger.fine("Processing DestroyRegionOperation, calling "
                + "reinitialize_destroy: " + fullPath);
          }

          // do just the destroy here, then spawn a thread to do the
          // the re-create. This allows the ack to be send for the
          // destroy message before we re-create the region.
          // Don't release the destroy lock until after we re-create
          rgn.reinitialize_destroy(ev);

          final LocalRegion loc_lockRoot = this.lockRoot;
          this.lockRoot = null; // spawned thread will release lock, not
                                // basicProcess

          rgn.getDistributionManager().getWaitingThreadPool().execute(
              new Runnable() {
                public void run()
                {
                  try {
                    rgn.reinitializeFromImageTarget(getSender());
                  }
                  catch (TimeoutException e) {
                    // dlock timed out, log message
                    rgn.getCache().getLoggerI18n().warning(
                        LocalizedStrings.DestroyRegionOperation_GOT_TIMEOUT_WHEN_TRYING_TO_RECREATE_REGION_DURING_REINITIALIZATION_1, 
                        rgn.getFullPath(), e);
                  }
                  catch (IOException e) {
                    // only if loading snapshot, not here
                    InternalGemFireError assErr = new InternalGemFireError(LocalizedStrings.UNEXPECTED_EXCEPTION.toLocalizedString());
                    assErr.initCause(e);
                    throw assErr;
                  }
                  catch (ClassNotFoundException e) {
                    // only if loading snapshot, not here
                    InternalGemFireError assErr = new InternalGemFireError(LocalizedStrings.UNEXPECTED_EXCEPTION.toLocalizedString());
                    assErr.initCause(e);
                    throw assErr;
                  }
                  finally {
                    if (loc_lockRoot != null)
                      loc_lockRoot.releaseDestroyLock();
                  }
                }
              });
        }
        else {
          if (logger.fineEnabled()) {
            logger.fine("Processing DestroyRegionOperation, calling "
                + "basicDestroyRegion: " + fullPath);
          }
          rgn.basicDestroyRegion(ev, false /* cacheWrite */, false /* lock */,
              true/* cacheCallbacks */);
        }
      }
      catch (CacheWriterException e) {
        throw new Error(LocalizedStrings.DestroyRegionOperation_CACHEWRITER_SHOULD_NOT_HAVE_BEEN_CALLED.toLocalizedString());
      }
      catch (TimeoutException e) {
        throw new Error(LocalizedStrings.DestroyRegionOperation_DISTRIBUTEDLOCK_SHOULD_NOT_HAVE_BEEN_ACQUIRED.toLocalizedString());
      }
      catch (RejectedExecutionException e) {
        // rejected while trying to execute recreate thread
        // must be shutting down, so what we were trying to do must not be
        // important anymore, so just quit
      }
    return true;
    }

    @Override
    protected boolean operateOnRegion(CacheEvent event, DistributionManager dm)
        throws EntryNotFoundException
    {
      Assert
          .assertTrue(false,
              LocalizedStrings.DestroyRegionOperation_REGION_DESTRUCTION_MESSAGE_IMPLEMENTATION_IS_IN_BASICPROCESS__NOT_THIS_METHOD.toLocalizedString());
      return false;
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append("; eventID=")
        .append(this.eventID)
        .append("; serialNum=")
        .append(this.serialNum)
        .append("; subregionSerialNumbers=")
        .append(this.subregionSerialNumbers)
        .append("; notifyOfRegionDeparture=")
        .append(this.notifyOfRegionDeparture);
    }

    public int getDSFID() {
      return DESTROY_REGION_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
        throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.eventID = DataSerializer.readObject(in);
      this.serialNum = DataSerializer.readPrimitiveInt(in);
      this.notifyOfRegionDeparture = DataSerializer.readPrimitiveBoolean(in);
      this.subregionSerialNumbers = DataSerializer.readHashMap(in);
    }

    @Override
    public void toData(DataOutput out)
        throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.eventID, out);
      DataSerializer.writePrimitiveInt(this.serialNum, out);
      DataSerializer.writePrimitiveBoolean(this.notifyOfRegionDeparture, out);
      DataSerializer.writeHashMap(this.subregionSerialNumbers, out);
    }
  }
  
  public static final class DestroyRegionWithContextMessage
    extends DestroyRegionMessage
   {
    protected transient Object context;

    @Override
    final public RegionEventImpl createRegionEvent(DistributedRegion rgn)
    {
      BridgeRegionEventImpl event = new BridgeRegionEventImpl(rgn,
          getOperation(), this.callbackArg, true /* originRemote */,
          getSender(), (ClientProxyMembershipID)this.context);
      return event;
    }

    @Override
    protected void appendFields(StringBuilder buff)
    {
      super.appendFields(buff);
      buff.append("; context=").append(this.context);
    }

    @Override
    public int getDSFID() {
      return DESTROY_REGION_WITH_CONTEXT_MESSAGE;
    }

    @Override
    public void fromData(DataInput in)
        throws IOException, ClassNotFoundException {
      super.fromData(in);
      this.context = DataSerializer.readObject(in);
    }

    @Override
    public void toData(DataOutput out)
        throws IOException {
      super.toData(out);
      DataSerializer.writeObject(this.context, out);
    }
  }
}
