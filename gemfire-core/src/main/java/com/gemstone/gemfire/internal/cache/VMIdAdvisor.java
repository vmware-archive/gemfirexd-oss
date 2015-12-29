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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.distributed.DistributedSystemDisconnectedException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;
import com.gemstone.gemfire.distributed.internal.membership.
    InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.locks.NonReentrantLock;
import com.gemstone.gemfire.internal.concurrent.ConcurrentTLongObjectHashMap;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.util.ArrayUtils;
import com.gemstone.gemfire.internal.util.concurrent.StoppableReentrantReadWriteLock;
import com.gemstone.gnu.trove.THashSet;

/**
 * This {@link CacheDistributionAdvisor} helps in assiging a distributed system
 * wide unique ID to the VM and keeps track of the same for other VMs in the
 * system. It tries to do this lazily to avoid any overheads in cache startup.
 * <p>
 * Unlike usual UUID generators, it does not depend on random numbers to be
 * unique (which is not a strong guarantee in any case), rather will use
 * messaging to give strong guarantees of uniqueness across the cluster and WAN
 * while ensuring minimal messaging (only once in {@link Integer#MAX_VALUE}
 * generations or so). Additionally it is much more compact than usual UUIDs
 * (e.g. split the long returned by newUUID versions returning long value).
 * <p>
 * The major use of this class is to generate globally unique IDs (UUIDs) by
 * combining this unique VM ID with a locally generated unique number. The
 * methods {@link #newUUID} will provide a new locally generated unique integer
 * combined with a DS-wide (as well as across WAN using the ID of this
 * distributed system) unique ID, that will automatically refresh the unique VM
 * ID if it overflows. These two integers are always generated sequentially so
 * users of this class may choose to invoke split the generated long into higher
 * and lower parts to compress them better during transportation or otherwise.
 * 
 * @author swale
 * @since 7.0
 */
public class VMIdAdvisor extends DistributionAdvisor {

  // ------------------------ Static Constants ----------------------

  /**
   * Indicates an invalid value of {@link #sequenceId} that will trigger request
   * for a new {@link #vmUniqueId}. Also stands for an invalid
   * {@link #vmUniqueId}.
   */
  public static final long INVALID_ID = -1L;
  public static final int INVALID_INT_ID = -1;

  /**
   * Default size of the maps.
   */
  public static final int DEFAULT_MAPSIZE = 103;

  /**
   * The name of {@link DistributedLockService} used as a global lock when
   * trying to grab a new VM ID.
   */
  private static final String DLOCK_SERVICE_NAME = "__VMID_LS";

  // below flags are for newUUID and newShortUUID
  public static final long UINT_MASK = 0xFFFFFFFFL;
  protected static final long INVALID_UUID = (INVALID_ID & UINT_MASK);
  protected static final int SHORT_UUID_IDBITS = 16;
  protected static final int SHORT_UUID_VMIDBITS =
    Integer.SIZE - SHORT_UUID_IDBITS;
  protected static final int SHORT_UUID_IDMASK = (1 << SHORT_UUID_IDBITS) - 1;
  public static final int INVALID_SHORT_ID = SHORT_UUID_IDMASK;
  protected static final int SHORT_UUID_MAX_VMID =
      (1 << (SHORT_UUID_VMIDBITS - 1)) - 1;

  protected static final int DSID_BITS = Byte.SIZE;
  protected static final int DSID_MAX = (1 << DSID_BITS) - 1;
  private static final long VMID_MAXPLUS1 = (1L << (Long.SIZE - DSID_BITS));
  protected static final long VMID_MAX = (VMID_MAXPLUS1 - 1);
  protected static final long VMID_MASK = VMID_MAX;

  private static final int VMIID_MAXPLUS1 = (1 << (Integer.SIZE - DSID_BITS));
  protected static final int VMIID_MAX = (VMIID_MAXPLUS1 - 1);

  // ---------------------- End: Static Constants -------------------

  private final InternalDistributedSystem sys;
  
  private final int dsID;
  

  /** read-write lock for the VM ID map */
  private final StoppableReentrantReadWriteLock mapLock;

  /**
   * Additional read-write lock for new member additions. This is separate from
   * {@link #mapLock} to avoid conditions like #50307.
   */
  private final StoppableReentrantReadWriteLock newMemberLock;

  /**
   * Current map of VM ID to the {@link InternalDistributedMember}.
   */
  private final ConcurrentTLongObjectHashMap<InternalDistributedMember>
      vmIdToMemberMap;

  /**
   * Current map of {@link InternalDistributedMember} to the VM ID.
   */
  private final ConcurrentHashMap<InternalDistributedMember, Long>
      memberToVMIdMap;

  /**
   * VM unique value in DS, used for creating globally unique VM ID.
   */
  protected volatile long vmUniqueId;

  /**
   * The current maximum VM ID in the distributed system.
   */
  protected final AtomicLong maxVMUniqueId;

  /**
   * AtomicInteger with vm unique value used for creating globally unqiue ID by
   * combining with a globally unique VM ID obtained using
   * {@link #fetchNewVMUniqueId}.
   */
  protected final AtomicInteger sequenceId;

  /**
   * AtomicInteger with vm unique value used for creating globally unqiue ID by
   * combining with a globally unique VM ID obtained using
   * {@link #fetchNewVMUniqueId}.
   * <p>
   * This one is used by {@link #newShortUUID()}.
   */
  protected final AtomicInteger sequenceIdForShortUUID;

  /**
   * {@link DistributedLockService} used when fetching a new VM ID (
   * {@link #fetchNewVMUniqueId}).
   */
  private volatile DistributedLockService lockService;

  /**
   * Used to synchronize threads when trying to get a new VM ID (
   * {@link #fetchNewVMUniqueId}).
   */
  private final NonReentrantLock vmIdLock;

  /** indicates fine level logging is enabled */
  private final boolean fineEnabled;

  /**
   * Constructs a new advisor. Also sets up this VM's profile and exchanges it
   * with other VMs.
   */
  protected VMIdAdvisor(InternalDistributedSystem system) {
    super(new VMIdAdvisee(system));
    this.sys = system;
    this.dsID = system.getDistributionManager().getDistributedSystemId();
    if (this.dsID > DSID_MAX) {
      throw new IllegalStateException("overflow with DSID=" + this.dsID);
    }
    this.mapLock = new StoppableReentrantReadWriteLock(false, true,
        system.getCancelCriterion());
    this.newMemberLock = new StoppableReentrantReadWriteLock(false, true,
        system.getCancelCriterion());
    this.vmIdToMemberMap = new ConcurrentTLongObjectHashMap<
        InternalDistributedMember>(4, DEFAULT_MAPSIZE);
    this.memberToVMIdMap = new ConcurrentHashMap<
        InternalDistributedMember, Long>(DEFAULT_MAPSIZE, 0.75f, 4);
    this.vmUniqueId = INVALID_ID;
    this.maxVMUniqueId = new AtomicLong(INVALID_ID);
    this.sequenceId = new AtomicInteger(INVALID_INT_ID);
    // initialize with max value to force VM unique ID to be generated the first
    // time if required
    this.sequenceIdForShortUUID = new AtomicInteger(INVALID_SHORT_ID);
    this.vmIdLock = new NonReentrantLock(true, system,
        system.getCancelCriterion());
    this.fineEnabled = system.getLogWriter().fineEnabled();
  }

  protected void subInit() {
    // set self as the advisor in VMIdAdvisee
    ((VMIdAdvisee)getAdvisee()).setVMIdAdvisor(this);
  }

  public static VMIdAdvisor createVMIdAdvisor(InternalDistributedSystem sys) {
    VMIdAdvisor advisor = new VMIdAdvisor(sys);
    advisor.initialize();
    return advisor;
  }

  /** Instantiate new distribution profile for this member */
  @Override
  protected VMIdProfile instantiateProfile(
      final InternalDistributedMember memberId, final int version) {
    return new VMIdProfile(memberId, version, this.vmUniqueId,
        this.maxVMUniqueId.get());
  }

  /**
   * @return true if new profile added, false if already had profile (but
   *         profile is still replaced with new one)
   */
  @Override
  protected final synchronized boolean basicAddProfile(final Profile p) {
    assert p instanceof VMIdProfile;

    boolean isAdded = false;
    boolean mapLockAcquired = false;
    final InternalDistributedMember memberId = p.getDistributedMember();
    this.newMemberLock.writeLock().lock();
    try {
      this.mapLock.writeLock().lock();
      mapLockAcquired = true;
      if (p instanceof VMIdProfile) {
        final VMIdProfile otherProfile = (VMIdProfile)p;
        final long otherVMId = otherProfile.getVMId();
        final long otherMaxVMId = otherProfile.getMaxVMId();
        final long thisMaxVMId = this.maxVMUniqueId.get();
        // invoke super's addProfile in any case otherwise an update to own
        // profile will not be sent to this member by adviseProfileUpdate
        isAdded = super.basicAddProfile(otherProfile);
        if (otherVMId == INVALID_ID) {
          // Other VM is starting up and initializing the advisor.
          // It will only collect the replies and it should not be registered
          // since that will be done lazily when the fetchNewVMUniqueId() is
          // invoked for the first time.
          // Other VM might have a valid maxVMId, so update from that.
          if (otherMaxVMId != INVALID_ID && (thisMaxVMId == INVALID_ID ||
                otherMaxVMId > thisMaxVMId)) {
            setMaxVMId(otherMaxVMId);
          }
          return true;
        }
        else if (this.vmUniqueId == INVALID_ID) {
          // this VM is initializing and receiving replies for VM ID,
          // OR is passively receiving updates from other nodes without
          // fetchNewVMUniqueId() having being invoked even once
          if (thisMaxVMId == INVALID_ID) {
            setMaxVMId(otherMaxVMId);
          }
          // there may be inconsistency since VMId of a new VM may have been
          // received by some nodes but not by others (#42543)
          // also can be different for recovery from disk
          /*
          else if (!pollIsInitialized()) {
            // we expect to receive consistent replies for maxVMId from all VMs
            Assert.assertTrue(thisMaxVMId == otherMaxVMId,
                "unexpected mismatch of maxIDs, thisMax=" + thisMaxVMId
                    + ", otherMax=" + otherMaxVMId + " for other VMID="
                    + otherVMId);
          }
          */
          else if (otherMaxVMId > thisMaxVMId) {
            // we could have initialized with an old max VMId from disk
            setMaxVMId(otherMaxVMId);
          }
          else if (otherVMId > thisMaxVMId) {
            // other VM is updating its VM ID
            setMaxVMId(otherVMId);
          }
        }
        else if (otherMaxVMId > thisMaxVMId) {
          // we could have initialized with an old max VMId from disk
          setMaxVMId(otherMaxVMId);
        }
        else if (otherVMId > thisMaxVMId) {
          // other VM is updating its VM ID
          setMaxVMId(otherVMId);
        }
        // update or insert the member's VMID to memberID mapping in both maps
        if (!isAdded) {
          updateMember(memberId, false);
        }
        this.vmIdToMemberMap.putPrimitive(otherVMId, memberId);
        this.memberToVMIdMap.put(memberId, otherVMId);
        if (this.fineEnabled) {
          getLogWriter().fine(toString() + ": added new VM ID " + otherVMId
              + " for " + memberId);
        }
      }
      else {
        Assert.fail(toString() + ": unexpected profile added: ("
            + p.getClass().getName() + ')' + p);
      }
    } finally {
      try {
        if (mapLockAcquired) {
          this.mapLock.writeLock().unlock();
        }
      } finally {
        this.newMemberLock.writeLock().unlock();
      }
    }
    return isAdded;
  }

  @Override
  public final synchronized boolean removeId(final ProfileId memberId,
      boolean crashed, boolean destroyed, boolean fromMembershipListener) {

    boolean removed = false;
    this.mapLock.writeLock().lock();
    try {
      if (memberId instanceof InternalDistributedMember) {
        super.removeId(memberId, crashed, destroyed, fromMembershipListener);
        removed = updateMember((InternalDistributedMember)memberId, true);
      }
      else {
        Assert.fail(toString() + ": unexpected profile ID to remove: ("
            + memberId.getClass().getName() + ") " + memberId);
      }
    } finally {
      this.mapLock.writeLock().unlock();
    }
    return removed;
  }

  /**
   * Acquiring this lock prevents new members from being added to the list of
   * members returned by {@link #getOtherMembers()}, or those returned by
   * {@link #adviseDistributedMember} or {@link #adviseVMId}.
   */
  public StoppableReentrantReadWriteLock getNewMemberLock() {
    return this.newMemberLock;
  }

  @SuppressWarnings("unchecked")
  public Set<InternalDistributedMember> getOtherMembers() {
    final InternalDistributedMember self = this.sys.getDistributedMember();
    final THashSet members = new THashSet(this.memberToVMIdMap.size());
    for (InternalDistributedMember m : this.memberToVMIdMap.keySet()) {
      if (!self.equals(m)) {
        members.add(m);
      }
    }
    return members;
  }

  private final boolean updateMember(final InternalDistributedMember memberId,
      boolean remove) {
    final Long currentId;
    if (remove) {
      currentId = this.memberToVMIdMap.remove(memberId);
    }
    else {
      currentId = this.memberToVMIdMap.get(memberId);
    }
    if (currentId != null) {
      final long id = currentId.longValue();
      final Object removedMember = this.vmIdToMemberMap.removePrimitive(id);
      if (!memberId.equals(removedMember)) {
        getLogWriter().error(LocalizedStrings.DEBUG,
            "expected removedMember " + removedMember + " to be same as "
                + memberId + " for VM ID " + id);
      }
      return true;
    }
    else {
      return false;
    }
  }

  @Override
  public void close() {
    try {
      // remove own profile from other VMs if initialized
      if (this.vmUniqueId != INVALID_ID) {
        new UpdateAttributesProcessor(getAdvisee(), true /*removeProfile*/)
            .distribute(false);
      }
    } catch (DistributedSystemDisconnectedException ignore) {
      // we are closing so ignore a shutdown exception.
    }
    super.close();
  }

  /**
   * Initialize the remote VM information explicitly and get a VMId for this
   * node (otherwise done lazily in {@link #newUUID},
   * {@link #adviseDistributedMember}, {@link #adviseVMId} methods).
   */
  public final void handshake() {
    if (!pollIsInitialized()) {
      initializationGate();
    }
  }

  /**
   * Get the current globally unique identifier for this VM. This may change if
   * the UUIDs being generated by {@link #newUUID} overflow the lower 32-bits.
   */
  public final long getVMUniqueId() {
    return getVMUniqueId(this.vmUniqueId);
  }

  public final long getVMUniqueId(long vmId) {
    if (this.dsID == -1) {
      return ((vmId & VMID_MASK));
    }
    else {
      return (((vmId & VMID_MASK) << DSID_BITS) | (this.dsID));
    }
  }

  /**
   * Get a new globally unique ID. The higher order 32-bits contain a unique ID
   * for the VM while the lower order bits contain a generated int unique in the
   * VM. If the generated ID overflows, then the VM's unique ID is refreshed.
   * 
   * @param throwOnOverflow
   *          if true then throw {@link IllegalStateException} when the UUIDs
   *          have been exhausted in the distributed system; note that it is not
   *          necessary that all possible long values would have been used by
   *          someone (e.g. a VM goes down without using its "block" of IDs)
   */
  public final long newUUID(final boolean throwOnOverflow)
      throws IllegalStateException {
    for (;;) {
      // -1 is an INVALID id;
      final int currentId = this.sequenceId.get();
      if (currentId != INVALID_INT_ID) {
        final long vmId = this.vmUniqueId;
        // check for overflow of VM ID
        if (throwOnOverflow && vmId > VMIID_MAX) {
          throw new IllegalStateException("long UUID overflow");
        }
        final int nextId = currentId + 1;
        if (compareAndSetSequenceId(currentId, nextId, false)) {
          final long uuid = ((nextId & UINT_MASK)
              | (getVMUniqueId(vmId) << Integer.SIZE));
          if (this.fineEnabled) {
            getLogWriter().fine(toString() + ": returning new UUID " + uuid);
          }
          return uuid;
        }
      }
      else {
        // if currentId is INVALID_ID, generate a new globally unique VM ID
        // under a lock

        // not taking the lock on "this" since that will deadlock if other VM
        // also goes for a DLock when collecting replies in fetchNewVMUniqueId()
        this.vmIdLock.lock();
        try {
          final long dsVMId = getNewHigherOrderIntegerNoLock(throwOnOverflow);
          if (dsVMId != INVALID_ID) {
            final long uuid = (1L | (dsVMId << Integer.SIZE));
            if (this.fineEnabled) {
              getLogWriter().fine(
                  toString() + ": returning new UUID " + uuid + "(new vmId "
                      + dsVMId + ')');
            }
            return uuid;
          }
        } finally {
          this.vmIdLock.unlock();
        }
      }
    }
  }

  /**
   * Get a new globally unique ID. The {@link ClusterUUID#getMemberId()} long
   * contains a unique ID for the VM in the distributed system and across
   * connected WAN while the {@link ClusterUUID#getUniqId()} contains a
   * generated int unique in the VM. If the generated ID overflows, then the
   * VM's unique ID is refreshed.
   */
  public final void newUUID(final ClusterUUID uuid) {
    for (;;) {
      // -1 is an INVALID id;
      final int currentId = this.sequenceId.get();
      if (currentId != INVALID_INT_ID) {
        final long vmId = this.vmUniqueId;
        final int nextId = currentId + 1;
        if (compareAndSetSequenceId(currentId, nextId, false)) {
          uuid.setUUID(getVMUniqueId(vmId), nextId, this);
          if (this.fineEnabled) {
            getLogWriter().fine(
                toString() + ": returning new ClusterUUID " + uuid);
          }
          return;
        }
      }
      else {
        // if currentId is INVALID_ID, generate a new globally unique VM ID
        // under a lock

        // not taking the lock on "this" since that will deadlock if other VM
        // also goes for a DLock when collecting replies in fetchNewVMUniqueId()
        this.vmIdLock.lock();
        try {
          final long dsVMId = getNewHigherOrderIntegerNoLock(false);
          if (dsVMId != INVALID_ID) {
            uuid.setUUID(dsVMId, 1, this);
            if (this.fineEnabled) {
              getLogWriter().fine(
                  toString() + ": returning new ClusterUUID " + uuid
                      + "(new vmId " + dsVMId + ')');
            }
            return;
          }
        } finally {
          this.vmIdLock.unlock();
        }
      }
    }
  }

  /** Get the VMId from generated DS memberID by {@link #newUUID} methods. */
  public final long getVMId(long memberId) {
    // VMID_MASK has already been applied by getVMUniqueId
    if (this.dsID == -1) {
      return memberId;
    }
    else {
      return (memberId >>> DSID_BITS);
    }
  }

  /**
   * Reset the UUID to given start value. It will return a value starting from
   * the value provided to this method and the next call to
   * {@link #newUUID(boolean)} will return a value greater than it.
   */
  public final long resetUUID(final long startValue)
      throws IllegalArgumentException, IllegalStateException {
    if (startValue >= 0) {
      long minVMId = ((startValue >>> Integer.SIZE) & UINT_MASK);
      long newSequenceId = (startValue & UINT_MASK);
      int newSequenceIdInt = (int)newSequenceId;
      if (newSequenceIdInt == INVALID_INT_ID) {
        minVMId++;
        newSequenceId = 0;
        newSequenceIdInt = 0;
      }
      if (minVMId > VMID_MAX) {
        throw new IllegalStateException("long UUID overflow");
      }
      long vmId = getVMUniqueId();
      // not taking the lock on "this" since that will deadlock if other VM
      // also goes for a DLock when collecting replies in fetchNewVMUniqueId()
      this.vmIdLock.lock();
      try {
        while (vmId < minVMId) {
          this.sequenceId.set(INVALID_INT_ID);
          this.sequenceIdForShortUUID.set(INVALID_SHORT_ID);
          vmId = getNewHigherOrderIntegerNoLock(true);
        }
        if (vmId == minVMId) {
          for (;;) {
            final int currentIdInt = this.sequenceId.get();
            final long currentId = (currentIdInt & UINT_MASK);
              if ( (newSequenceId != 0 ) && ( newSequenceId <= currentId )) {
              final long uuid;
              if (currentIdInt != INVALID_INT_ID) {
                uuid = (currentId | (vmId << Integer.SIZE));
              }
              else {
                uuid = newUUID(true);
              }
              if (this.fineEnabled) {
                getLogWriter().fine(
                    toString() + ": returning reset UUID " + uuid);
              }
              return uuid;
            }
            else if (compareAndSetSequenceId(currentIdInt, newSequenceIdInt,
                true)) {
              final long uuid = (newSequenceId | (vmId << Integer.SIZE));
              if (this.fineEnabled) {
                getLogWriter().fine(
                    toString() + ": returning reset UUID " + uuid);
              }
              return uuid;
            }
          }
        }
        else {
          if (vmId <= minVMId) {
            Assert.fail("unexpected vmId=" + vmId + ", minVMId=" + minVMId);
          }
          final long uuid = newUUID(true);
          if (this.fineEnabled) {
            getLogWriter().fine(toString() + ": returning reset UUID " + uuid);
          }
          return uuid;
        }
      } finally {
        this.vmIdLock.unlock();
      }
    }
    else {
      throw new IllegalArgumentException("invalid start value " + startValue);
    }
  }

  /**
   * Get a short unique ID that may or may not be globally unique with large
   * number of servers. The higher order bits contain an DSID and ID for the VM
   * while the lower order {@value #SHORT_UUID_IDBITS} bits contain a generated
   * int unique in the VM. If the generated ID overflows, then the VM's unique
   * ID is refreshed. However, if even the VM's unique ID overflows then an
   * {@link IllegalStateException} is thrown.
   * <p>
   * It is recommended to always use {@link #newUUID} -- use this only if you
   * have to.
   * 
   * @throws IllegalStateException
   *           thrown when UUIDs have been exhaused in the distributed system;
   *           note that it is not necessary that all possible integer values
   *           would have been used by someone (e.g. a VM goes down without
   *           using its "block" of IDs)
   */
  public final int newShortUUID() throws IllegalStateException {
    for (;;) {
      final int currentId = this.sequenceIdForShortUUID.get();
      // cannot go beyond maximum allowable value
      if (currentId != INVALID_SHORT_ID) {
        final long vmId = this.vmUniqueId;
        // check for overflow of VM ID
        if (vmId < 0 || vmId > SHORT_UUID_MAX_VMID) {
          throw new IllegalStateException("int UUID overflow");
        }
        final int nextId = currentId + 1;
        if (compareAndSetSequenceIdForShortUUID(currentId, nextId, false)) {
          final int uuid = ((nextId & SHORT_UUID_IDMASK) |
              ((int)vmId << SHORT_UUID_IDBITS));
          if (this.fineEnabled) {
            getLogWriter().fine(
                toString() + ": returning new short UUID " + uuid);
          }
          return uuid;
        }
      }
      else {
        // if currentId has reached maximum, generate a new globally unique VM
        // ID under a lock

        // not taking the lock on "this" since that will deadlock if other VM
        // also goes for a DLock when collecting replies in fetchNewVMUniqueId()
        this.vmIdLock.lock();
        try {
          final int vmId = getNewVMIdShortNoLock();
          if (vmId != INVALID_INT_ID) {
            final int uuid = (1 | (vmId << SHORT_UUID_IDBITS));
            if (this.fineEnabled) {
              getLogWriter().fine(
                  toString() + ": returning new short UUID " + uuid
                      + "(new vmId " + vmId + ')');
            }
            return uuid;
          }
        } finally {
          this.vmIdLock.unlock();
        }
      }
    }
  }

  /**
   * Reset the short UUID to given start value. It will return a value starting
   * from the value provided to this method and the next call to
   * {@link #newShortUUID()} will return a value greater than it.
   */
  public final int resetShortUUID(final int startValue)
      throws IllegalArgumentException, IllegalStateException {
    if (startValue >= 0) {
      int minVMId = (startValue >>> SHORT_UUID_IDBITS);
      int newSequenceId = (startValue & SHORT_UUID_IDMASK);
      if (newSequenceId == INVALID_SHORT_ID) {
        minVMId++;
        newSequenceId = 0;
      }
      if (minVMId < 0 || minVMId > SHORT_UUID_MAX_VMID) {
        throw new IllegalStateException("int UUID overflow");
      }
      long vmId = this.vmUniqueId;
      // not taking the lock on "this" since that will deadlock if other VM
      // also goes for a DLock when collecting replies in fetchNewVMUniqueId()
      this.vmIdLock.lock();
      try {
        while (vmId < minVMId) {
          this.sequenceId.set(INVALID_INT_ID);
          this.sequenceIdForShortUUID.set(INVALID_SHORT_ID);
          vmId = getNewVMIdShortNoLock();
        }
        if (vmId == minVMId) {
          for (;;) {
            final int currentId = this.sequenceIdForShortUUID.get();
            if ( (newSequenceId != 0 ) && ( newSequenceId <= currentId )) {
              final int uuid;
              if (currentId != INVALID_SHORT_ID) {
                uuid = (currentId | ((int)vmId << SHORT_UUID_IDBITS));
              }
              else {
                uuid = newShortUUID();
              }
              if (this.fineEnabled) {
                getLogWriter().fine(
                    toString() + ": returning reset short UUID " + uuid);
              }
              return uuid;
            }
            else if (compareAndSetSequenceIdForShortUUID(currentId,
                newSequenceId, true)) {
              final int uuid = (newSequenceId
                  | ((int)vmId << SHORT_UUID_IDBITS));
              if (this.fineEnabled) {
                getLogWriter().fine(
                    toString() + ": returning reset short UUID " + uuid);
              }
              return uuid;
            }
          }
        }
        else {
          if (vmId <= minVMId) {
            Assert.fail("unexpected vmId=" + vmId + ", minVMId=" + minVMId);
          }
          final int uuid = newShortUUID();
          if (this.fineEnabled) {
            getLogWriter().fine(
                toString() + ": returning reset short UUID " + uuid);
          }
          return uuid;
        }
      } finally {
        this.vmIdLock.unlock();
      }
    }
    else {
      throw new IllegalArgumentException("invalid start value " + startValue);
    }
  }

  /**
   * Returns the {@link InternalDistributedMember} with the given VM ID.
   * 
   * @param vmUniqueId
   *          the VM ID of the required member
   * @param initializeAdvisor
   *          initialize the advisor to obtain the VM IDs of other VMs if
   *          required
   */
  public final InternalDistributedMember adviseDistributedMember(
      final long vmUniqueId, final boolean initializeAdvisor) {

    if (initializeAdvisor) {
      // initialize lazily to get others' profiles if not done already
      handshake();
    }

    // check for self
    if (this.vmUniqueId == vmUniqueId) {
      return this.sys.getDistributedMember();
    }

    // check for others
    return (InternalDistributedMember)this.vmIdToMemberMap
        .getPrimitive(vmUniqueId);
  }

  /**
   * Returns the VM ID of the given {@link InternalDistributedMember}.
   * 
   * @param memberId
   *          the {@link InternalDistributedMember} of the required member
   * @param initializeAdvisor
   *          initialize the advisor to obtain the VM IDs of other VMs if
   *          required
   */
  public final long adviseVMId(final InternalDistributedMember memberId,
      final boolean initializeAdvisor) {

    if (initializeAdvisor) {
      // initialize lazily to get others' profiles if not done already
      handshake();
    }

    // check for others
    Long id = this.memberToVMIdMap.get(memberId);
    if (id != null) {
      return id.longValue();
    }

    // check for self
    if (this.sys.getDistributedMember().equals(memberId)) {
      return getVMUniqueId();
    }
    return INVALID_ID;
  }

  /**
   * Get a new VM ID for {@link #newUUID(boolean)} using
   * {@link #fetchNewVMUniqueId} under {@link #vmIdLock} throwing exception
   * for overflow of VM ID when "throwOnOverflow" parameter is set to true.
   */
  private long getNewHigherOrderIntegerNoLock(final boolean throwOnOverflow)
      throws IllegalStateException {
    if (this.sequenceId.get() == INVALID_ID) {
      // sequenceId will already have been reset in fetchNewVMUniqueId
      fetchNewVMUniqueId(throwOnOverflow ? VMIID_MAX : -1);
      return getVMUniqueId();
    }
    return INVALID_ID;
  }

  private int getNewVMIdShortNoLock() throws IllegalStateException {
    if (this.sequenceIdForShortUUID.get() == INVALID_SHORT_ID) {
      fetchNewVMUniqueId(SHORT_UUID_MAX_VMID);
      // sequenceIdForShortUUID will already have been reset in
      // fetchNewVMUniqueId
      return (int)this.vmUniqueId;
    }
    return INVALID_INT_ID;
  }

  protected boolean compareAndSetSequenceId(final int expect, final int update,
      final boolean reset) {
    return this.sequenceId.compareAndSet(expect, update);
  }

  protected boolean compareAndSetSequenceIdForShortUUID(final int expect,
      final int update, final boolean reset) {
    return this.sequenceIdForShortUUID.compareAndSet(expect, update);
  }

  protected void setNewVMId(final long newVMId, long setMaxVMId) {
    this.vmUniqueId = newVMId;
  }

  protected void setMaxVMId(final long maxVMId) {
    this.maxVMUniqueId.set(maxVMId);
  }

  /**
   * Get a new globally uniqueId for this VM and reset the VM local UUID
   * generator AtomicIntegers ({@link #sequenceId} and
   * {@link #sequenceIdForShortUUID}). Called whenever this VM's
   * {@link #sequenceId}/{@link #sequenceIdForShortUUID} wraps over to maximum
   * allowable value.
   */
  protected final void fetchNewVMUniqueId(final long vmIdMax)
      throws IllegalStateException {
    // assumed to be invoked under lock
    Assert.assertTrue(this.vmIdLock.isLocked());

    // check for overflow
    final long currentMaxVMId = this.maxVMUniqueId.get();
    if ((vmIdMax > 0 && currentMaxVMId >= vmIdMax)
        || ((currentMaxVMId + 1) > VMID_MAX)) {
      throw new IllegalStateException("UUID overflow");
    }
    final LogWriterI18n logger = getLogWriter();
    initVMIDLockService();
    final String dlockName = getDLockName();
    for (int n = 1;; ++n) {
      if (this.fineEnabled) {
        logger.fine(toString() + ": Trying to get the dlock for object '"
            + dlockName + "' iteration=" + n);
      }
      if (this.lockService.lock(dlockName,
          PartitionedRegion.VM_OWNERSHIP_WAIT_TIME, -1)) {
        try {
          // try to initialize and grab others' profiles the first time, else
          // update
          if (initializationGate()) {
            Assert.assertTrue(this.vmUniqueId == INVALID_ID);
          }
          // initialize by incrementing the current max VM ID either as received
          // from others' during initial profile exchange or as tracked in
          // updates later; the DLock ensures that no other VM is trying to do
          // the same thing concurrently
          final long newVMId = this.maxVMUniqueId.incrementAndGet();
          setNewVMId(newVMId, newVMId);
          if (this.fineEnabled) {
            logger.fine(toString() + ": Assigned new VMId=" + this.vmUniqueId
                + " dsID=" + this.dsID);
          }
          // distribute the updated profile to other VMs
          new UpdateAttributesProcessor(getAdvisee()).distribute(false);
          break;
        } finally {
          try {
            this.lockService.unlock(dlockName);
            if (this.fineEnabled) {
              logger.fine(toString() + ": Released the dlock for object '"
                  + dlockName + "'");
            }
          } catch (Exception ex) {
            this.sys.getCancelCriterion().checkCancelInProgress(null);
            if (this.fineEnabled) {
              logger.fine("unlocking object '" + dlockName
                  + "' caught an exception", ex);
            }
          }
        }
      }
      this.sys.getCancelCriterion().checkCancelInProgress(null);
    }
    boolean seqIdCAS = compareAndSetSequenceId(INVALID_INT_ID, 1, true);
    boolean seqIdForShortCAS = compareAndSetSequenceIdForShortUUID(
        INVALID_SHORT_ID, 1, true);
    if (!seqIdCAS && !seqIdForShortCAS) {
      Assert.fail("expected ID value " + INVALID_ID + " or short ID value "
          + INVALID_SHORT_ID + " but values found " + this.sequenceId.get()
          + ", " + this.sequenceIdForShortUUID.get() + " respectively");
    }
  }

  /**
   * Initialize the lock service used by {@link #fetchNewVMUniqueId} to get a
   * new VM ID. Should be invoked under a lock.
   */
  protected void initVMIDLockService() {
    this.sys.getCancelCriterion().checkCancelInProgress(null);
    if (this.lockService == null) {
      try {
        this.lockService = DLockService
            .create(DLOCK_SERVICE_NAME, this.sys, true /*distributed*/,
                true /*destroyOnDisconnect*/, true /*automateFreeResources*/);
      } catch (IllegalArgumentException e) {
        this.lockService = DistributedLockService
            .getServiceNamed(DLOCK_SERVICE_NAME);
        if (this.lockService == null) {
          throw e; // DLOCK_SERVICE_NAME must be illegal!
        }
      }
    }
  }

  /**
   * Name of the distributed object that is locked using
   * {@link DistributedLockService} when trying to grab a new VM ID.
   */
  protected String getDLockName() {
    return "NEWID";
  }

  protected CancelCriterion getCancelCriterion(
      final InternalDistributedSystem sys) {
    return sys.getCancelCriterion();
  }

  @Override
  public String toString() {
    return ArrayUtils.objectRefString(this);
  }

  /**
   * Profile used for exchanging this VM's and max VM ID information with other
   * members in the distributed system.
   * 
   * @author swale
   * @since 7.0
   */
  public static class VMIdProfile extends DistributionAdvisor.Profile implements
      DataSerializableFixedID {

    /**
     * The unique VM ID that is the same as this VM's
     * {@link VMIdAdvisor#vmUniqueId}.
     */
    private long vmId;

    /**
     * The maximum VM ID seen so far by this VM (
     * {@link VMIdAdvisor#maxVMUniqueId}).
     */
    private long maxVMId;

    /** for deserialization */
    public VMIdProfile() {
    }

    /** construct a new instance for given member and with given version */
    public VMIdProfile(final InternalDistributedMember memberId,
        final int version, final long id, final long maxId) {
      super(memberId, version);
      this.vmId = id;
      this.maxVMId = maxId;
    }

    public final long getVMId() {
      return this.vmId;
    }

    public final long getMaxVMId() {
      return this.maxVMId;
    }

    @Override
    public void processIncoming(final DistributionManager dm,
        final String adviseePath, final boolean removeProfile,
        final boolean exchangeProfiles, final List<Profile> replyProfiles,
        final LogWriterI18n logger) {
      final InternalDistributedSystem sys = InternalDistributedSystem
          .getConnectedInstance();
      final VMIdAdvisor advisor;
      if (sys != null && (advisor = sys.getVMIdAdvisor()) != null) {
        // exchange our profile even if not initialized so that the receiver
        // records this VM and continues to send updates to its own profile
        handleDistributionAdvisee(advisor.getAdvisee(), removeProfile,
            exchangeProfiles, replyProfiles);
      }
    }

    @Override
    public int getDSFID() {
      return VMID_PROFILE_MESSAGE;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      InternalDataSerializer.writeSignedVL(this.vmId, out);
      InternalDataSerializer.writeSignedVL(this.maxVMId, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      super.fromData(in);
      this.vmId = InternalDataSerializer.readSignedVL(in);
      this.maxVMId = InternalDataSerializer.readSignedVL(in);
    }

    @Override
    public StringBuilder getToStringHeader() {
      return new StringBuilder("VMIdProfile");
    }

    @Override
    public void fillInToString(StringBuilder sb) {
      super.fillInToString(sb);
      sb.append("; vmId=").append(this.vmId);
      sb.append("; maxVMId=").append(this.maxVMId);
    }
  }

  /**
   * A {@link DistributionAdvisee} implementation for {@link VMIdAdvisor}.
   * Really does nothing apart from satisfying the interface and delegating
   * everything to {@link VMIdAdvisor}.
   * 
   * @author swale
   * @since 7.0
   */
  private static final class VMIdAdvisee implements DistributionAdvisee {

    /**
     * Serial number used for {@link DistributionAdvisee#getSerialNumber()}.
     */
    private final int serialNumber = DistributionAdvisor.createSerialNumber();

    private final InternalDistributedSystem sys;

    private VMIdAdvisor advisor;

    VMIdAdvisee(final InternalDistributedSystem sys) {
      this.sys = sys;
    }

    final void setVMIdAdvisor(final VMIdAdvisor advisor) {
      this.advisor = advisor;
    }

    // ------------------------ DistributionAdvisee implementation begin

    public void fillInProfile(final Profile p) {
      assert p instanceof VMIdProfile;

      final VMIdProfile profile = (VMIdProfile)p;
      profile.serialNumber = getSerialNumber();
    }

    public VMIdAdvisor getDistributionAdvisor() {
      return this.advisor;
    }

    public Profile getProfile() {
      return getDistributionAdvisor().createProfile();
    }

    public InternalDistributedSystem getSystem() {
      return this.sys;
    }

    public CancelCriterion getCancelCriterion() {
      return this.advisor.getCancelCriterion(this.sys);
    }

    public DM getDistributionManager() {
      return getSystem().getDistributionManager();
    }

    public final String getName() {
      return "VMIdAdvisee";
    }

    public final String getFullPath() {
      return getName();
    }

    public DistributionAdvisee getParentAdvisee() {
      return null;
    }

    public int getSerialNumber() {
      return this.serialNumber;
    }

    // ------------------------ DistributionAdvisee implementation end
  }
}
