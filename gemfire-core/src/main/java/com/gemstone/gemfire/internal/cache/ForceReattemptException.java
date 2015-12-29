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


import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;

/**
 * Indicates that the current partitioned region operation failed.  It
 * is only thrown in a context where the higher level operation needs to
 * instigate a retry of some sort.
 * 
 * @see com.gemstone.gemfire.internal.cache.partitioned.PartitionMessage
 * @author mthomas
 * @since 5.0
 */
public class ForceReattemptException extends
    DataLocationException
{
  private static final long serialVersionUID = -595988965679204903L;
  /**
   * If true, this exception includes a hashCode for specified key
   */
  private boolean hasHash = false;
  
  /**
   * The hashCode for a specified key, if {@link #hasHash()} is true
   */
  private int keyHash = 0;

  /** member where this exception originated */
  private InternalDistributedMember origin;

  /**
   * Used when constructing the error: sets the expected hash.
   * @param h the hash to use
   */
  public void setHash(int h) {
    Assert.assertTrue(!this.hasHash, "setHash already called");
    this.hasHash = true;
    this.keyHash = h;
  }
  
  /**
   * Fetch the hash for this exception
   * @return the expected hash
   */
  public boolean hasHash() {
    return this.hasHash;
  }
  
  public int getHash() {
    if (!hasHash) {
      throw new InternalGemFireError("getHash when no hash");
    }
    return this.keyHash;
  }
  
  /**
   * If possible, validate the given key's hashCode against any
   * that was returned by the peer.
   * @param key the key on the current host.  If null, no check is done.
   * @throws PartitionedRegionException if the keys disagree.
   */
  public void checkKey(Object key) throws PartitionedRegionException {
    if (!hasHash) {
      return; // none provided
    }
    if (key == null) {
      return; // ???
    }
    
    int expected = key.hashCode();
    if (expected == keyHash) {
      return;
    }
    throw new PartitionedRegionException(
        "Object hashCode inconsistent between cache peers. Here = " + 
        expected + "; peer calculated = " + keyHash + " for " + key);
  }
  
  /**
   * Reattempt required due to an underlying error
   * 
   * @param message describes the context
   * @param cause the underlying cause
   */
  public ForceReattemptException(String message, Throwable cause) {
    super(message, cause);
    setMyId();
  }
  
  /**
   * Reattempt required due to detected condition
   * @param message describes the condition
   */
  public ForceReattemptException(String message) {
    super(message);
    setMyId();
  }

  public final InternalDistributedMember getOrigin() {
    return this.origin;
  }

  private void setMyId() {
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      this.origin = cache.getMyId();
    }
  }

  @Override
  public String toString() {
    String result = super.toString();
    if (hasHash()) {
      result = result + " (hash = " + keyHash + ")";
    }
    return result;
  }
}
