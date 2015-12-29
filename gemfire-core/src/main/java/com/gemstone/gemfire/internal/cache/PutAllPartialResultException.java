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

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.internal.cache.tier.sockets.VersionedObjectList;
import java.io.Serializable;
import java.util.*;

/**
 * This exception is thrown as some keys in map failed in putAll operation.
 *
 * @author xzhou
 *
 *
 * @since 6.5
 */
public class PutAllPartialResultException extends GemFireException {

  private PutAllPartialResult result;

  ////////////////////  Constructors  ////////////////////

  /**
   * Creates a new <code>PutAllPartialResultException</code>
   */
  public PutAllPartialResultException(PutAllPartialResult result) {
    super("Partial keys are processed in putAll", result != null ? result
        .getFailure() : null);
    this.result = result;
  }

  public PutAllPartialResultException() {
    super("Partial keys are processed in putAll");
    result = new PutAllPartialResult(-1);
  }

  /**
   * consolidate exceptions
   */
  public void consolidate(PutAllPartialResultException pre) {
    this.result.consolidate(pre.getResult());
  }
  
  public void consolidate(PutAllPartialResult otherResult) {
    this.result.consolidate(otherResult);
  }
  
  public PutAllPartialResult getResult() {
    return this.result;
  }
  
  /**
   * Returns the key set in exception
   */
  public VersionedObjectList getSucceededKeysAndVersions() {
    return this.result.getSucceededKeysAndVersions();
  }

  public Exception getFailure() {
    return this.result.getFailure();
  }
  
  /**
   * Returns there's failedKeys
   */
  public boolean hasFailure() {
    return this.result.hasFailure();
  }

  public Object getFirstFailedKey() {
    return this.result.getFirstFailedKey();
  }

  @Override
  public String getMessage() {
    return this.result.toString();
  }

  public static class PutAllPartialResult implements Serializable {
    private VersionedObjectList succeededKeys;
    private Object firstFailedKey;
    private Exception firstCauseOfFailure;
    private int totalMapSize;

    ////////////////////  Constructors  ////////////////////

    /**
     * Creates a new <code>PutAllPartialResult</code>
     */
    public PutAllPartialResult(int totalMapSize) {
      this.succeededKeys = new VersionedObjectList();
      this.totalMapSize = totalMapSize;
    }

    public void setTotalMapSize(int totalMapSize) {
      this.totalMapSize = totalMapSize;
    }
    
    public void consolidate(PutAllPartialResult other) {
      synchronized(this.succeededKeys) {
        this.succeededKeys.addAll(other.getSucceededKeysAndVersions());
      }
      saveFailedKey(other.firstFailedKey, other.firstCauseOfFailure);
    }

    public Exception getFailure() {
      return this.firstCauseOfFailure;
    }
    
    /**
     * record all succeeded keys in a version list response
     */
    public void addKeysAndVersions(VersionedObjectList keysAndVersions) {
      synchronized(this.succeededKeys){ 
        this.succeededKeys.addAll(keysAndVersions);
    }
    }
    
    /**
     * record all succeeded keys when there are no version results 
     */
    public void addKeys(Collection<Object> keys) {
      synchronized(this.succeededKeys) {
        if (this.succeededKeys.getVersionTags().size() > 0) {
          throw new IllegalStateException("attempt to store versionless keys when there are already versioned results");
        }
        this.succeededKeys.addAllKeys(keys);
      }
    }
    

    /**
     * increment failed key number
     */
    public void saveFailedKey(Object key, Exception cause) {
      if (key == null) {
        return;
      }
      // we give high priority for CancelException
      if (firstFailedKey == null || cause instanceof CancelException) {
        firstFailedKey = key;
        firstCauseOfFailure = cause;
      }
    }

    /**
     * Returns the key set in exception
     */
    public VersionedObjectList getSucceededKeysAndVersions() {
      return this.succeededKeys;
    }

    /**
     * Returns the first key that failed
     */
    public Object getFirstFailedKey() {
      return this.firstFailedKey;
    }

    /**
     * Returns there's failedKeys
     */
    public boolean hasFailure() {
      return this.firstFailedKey != null;
    }

    /**
     * Returns there's saved succeed keys
     */
    public boolean hasSucceededKeys() {
      return this.succeededKeys.size() > 0;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder("Key "
          +firstFailedKey+" and possibly others failed to put due to "+firstCauseOfFailure+"\n");
      if (totalMapSize > 0) {
        int failedKeyNum = totalMapSize - this.succeededKeys.size();
        sb.append("The putAll operation failed to put "+failedKeyNum 
            + " out of " + totalMapSize 
            + " entries. ");
      }
      return sb.toString();
    }
    
    public String detailString() {
      StringBuilder sb = new StringBuilder(toString());
      sb.append(getKeyListString());
      return sb.toString();
    }

    public String getKeyListString() {
      StringBuilder sb = new StringBuilder();
      
      sb.append("The keys for the successfully put entries are: ");
      int cnt = 0;
      final Iterator iterator = this.succeededKeys.iterator();
      while (iterator.hasNext()) {
        Object key = iterator.next();
        sb.append(" ").append(key);
        cnt++;
      }
      return sb.toString();
    }
  }
}
