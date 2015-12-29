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
package hct; 

import util.*;

public class ExpectedRegionContents implements Cloneable {

// instance fields
// KeyIntevals.NONE
   private Boolean containsKey_none;            // expected value of containsKey for KeyIntervals.NONE
   private Boolean containsValue_none;          // expected value of containsValue for KeyIntervals.NONE
   private boolean getAllowed_none;             // if true, then check the value with a get

// KeyIntevals.INVALIDATE
   private Boolean containsKey_invalidate;
   private Boolean containsValue_invalidate;
   private boolean getAllowed_invalidate;

// KeyIntevals.LOCAL_INVALIDATE
   private Boolean containsKey_localInvalidate;
   private Boolean containsValue_localInvalidate;
   private boolean getAllowed_localInvalidate;

// KeyIntevals.DESTROY
   private Boolean containsKey_destroy;
   private Boolean containsValue_destroy;
   private boolean getAllowed_destroy;

// KeyIntevals.LOCAL_DESTROY
   private Boolean containsKey_localDestroy;
   private Boolean containsValue_localDestroy;
   private boolean getAllowed_localDestroy;

// KeyIntevals.UPDATE
   private Boolean containsKey_update;
   private Boolean containsValue_update;
   private boolean getAllowed_update;
   private boolean valueIsUpdated;

// KeyIntervals.GET
   private Boolean containsKey_get;
   private Boolean containsValue_get;
   private boolean getAllowed_get;

// new keys
   private Boolean containsKey_newKey;
   private Boolean containsValue_newKey;
   private boolean getAllowed_newKey;

// region size specifications
private Integer exactSize = null;
private Integer minSize = null;
private Integer maxSize = null;

// constructors
public ExpectedRegionContents(boolean containsKey, boolean containsValue, boolean getAllowed) {
   containsKey_none = new Boolean(containsKey);
   containsKey_invalidate = new Boolean(containsKey);
   containsKey_localInvalidate = new Boolean(containsKey);
   containsKey_destroy = new Boolean(containsKey);
   containsKey_localDestroy = new Boolean(containsKey);
   containsKey_update = new Boolean(containsKey);
   containsKey_get = new Boolean(containsKey);
   containsKey_newKey = new Boolean(containsKey);

   containsValue_none = new Boolean(containsValue);
   containsValue_invalidate = new Boolean(containsValue);
   containsValue_localInvalidate = new Boolean(containsValue);
   containsValue_destroy = new Boolean(containsValue);
   containsValue_localDestroy = new Boolean(containsValue);
   containsValue_update = new Boolean(containsValue);
   containsValue_get = new Boolean(containsValue);
   containsValue_newKey = new Boolean(containsValue);

   getAllowed_none = getAllowed;
   getAllowed_invalidate = getAllowed;
   getAllowed_localInvalidate = getAllowed;
   getAllowed_destroy = getAllowed;
   getAllowed_localDestroy = getAllowed;
   getAllowed_update = getAllowed;
   getAllowed_get = getAllowed;
   getAllowed_newKey = getAllowed;
   valueIsUpdated = false;

}

public ExpectedRegionContents(boolean containsKeyNone,            boolean containsValueNone, 
                              boolean containsKeyInvalidate,      boolean containsValueInvalidate,
                              boolean containsKeyLocalInvalidate, boolean containsValueLocalInvalidate,
                              boolean containsKeyDestroy,         boolean containsValueDestroy,
                              boolean containsKeyLocalDestroy,    boolean containsValueLocalDestroy,
                              boolean containsKeyUpdate,          boolean containsValueUpdate,
                              boolean containsKeyGet,             boolean containsValueGet,
                              boolean containsKeyNewKey,          boolean containsValueNewKey,
                              boolean getAllowed,
                              boolean updated) {
   containsKey_none = new Boolean(containsKeyNone);
   containsValue_none = new Boolean(containsValueNone);

   containsKey_invalidate = new Boolean(containsKeyInvalidate);
   containsValue_invalidate = new Boolean(containsValueInvalidate);

   containsKey_localInvalidate = new Boolean(containsKeyLocalInvalidate);
   containsValue_localInvalidate = new Boolean(containsValueLocalInvalidate);

   containsKey_destroy = new Boolean(containsKeyDestroy);
   containsValue_destroy = new Boolean(containsValueDestroy);

   containsKey_localDestroy = new Boolean(containsKeyLocalDestroy);
   containsValue_localDestroy = new Boolean(containsValueLocalDestroy);

   containsKey_update = new Boolean(containsKeyUpdate);
   containsValue_update = new Boolean(containsValueUpdate);

   containsKey_get = new Boolean(containsKeyGet);
   containsValue_get = new Boolean(containsValueGet);

   containsKey_newKey = new Boolean(containsKeyNewKey);
   containsValue_newKey = new Boolean(containsValueNewKey);

   getAllowed_none = getAllowed;
   getAllowed_invalidate = getAllowed;
   getAllowed_localInvalidate = getAllowed;
   getAllowed_destroy = getAllowed;
   getAllowed_localDestroy = getAllowed;
   getAllowed_update = getAllowed;
   getAllowed_get = getAllowed;
   getAllowed_newKey = getAllowed;
   valueIsUpdated = updated;
}

//================================================================================
// getter methods
public Boolean containsKey_none() {
   return containsKey_none;
}
public Boolean containsValue_none() {
   return containsValue_none;
}
public boolean getAllowed_none() {
   return getAllowed_none;
}
public Boolean containsKey_invalidate() {
   return containsKey_invalidate;
}
public Boolean containsValue_invalidate() {
   return containsValue_invalidate;
}
public boolean getAllowed_invalidate() {
   return getAllowed_invalidate;
}
public Boolean containsKey_localInvalidate() {
   return containsKey_localInvalidate;
}
public Boolean containsValue_localInvalidate() {
   return containsValue_localInvalidate;
}
public boolean getAllowed_localInvalidate() {
   return getAllowed_localInvalidate;
}
public Boolean containsKey_destroy() {
   return containsKey_destroy;
}
public Boolean containsValue_destroy() {
   return containsValue_destroy;
}
public boolean getAllowed_destroy() {
   return getAllowed_destroy;
}
public Boolean containsKey_localDestroy() {
   return containsKey_localDestroy;
}
public Boolean containsValue_localDestroy() {
   return containsValue_localDestroy;
}
public boolean getAllowed_localDestroy() {
   return getAllowed_localDestroy;
}
public Boolean containsKey_update() {
   return containsKey_update;
}
public Boolean containsValue_update() {
   return containsValue_update;
}
public boolean getAllowed_update() {
   return getAllowed_update;
}
public boolean valueIsUpdated() {
   return valueIsUpdated;
}
public Boolean containsKey_get() {
   return containsKey_get;
}
public Boolean containsValue_get() {
   return containsValue_get;
}
public boolean getAllowed_get() {
   return getAllowed_get;
}
public Boolean containsKey_newKey() {
   return containsKey_newKey;
}
public Boolean containsValue_newKey() {
   return containsValue_newKey;
}
public boolean getAllowed_newKey() {
   return getAllowed_newKey;
}
public Integer exactSize() {
   return exactSize;
}
public Integer minSize() {
   return minSize;
}
public Integer maxSize() {
   return maxSize;
}

//================================================================================
// setter methods
public void containsKey_none(boolean abool) {
   containsKey_none = new Boolean(abool);
}
public void containsKey_none(Boolean abool) {
   containsKey_none = abool;
}
public void containsValue_none(boolean abool) {
   containsValue_none = new Boolean(abool);
}
public void containsValue_none(Boolean abool) {
   containsValue_none = abool;
}
public void containsKey_invalidate(boolean abool) {
   containsKey_invalidate = new Boolean(abool);
}
public void containsKey_invalidate(Boolean abool) {
   containsKey_invalidate = abool;
}
public void containsValue_invalidate(boolean abool) {
   containsValue_invalidate = new Boolean(abool);
}
public void containsValue_invalidate(Boolean abool) {
   containsValue_invalidate = abool;
}
public void containsKey_localInvalidate(boolean abool) {
   containsKey_localInvalidate = new Boolean(abool);
}
public void containsKey_localInvalidate(Boolean abool) {
   containsKey_localInvalidate = abool;
}
public void containsValue_localInvalidate(boolean abool) {
   containsValue_localInvalidate = new Boolean(abool);
}
public void containsValue_localInvalidate(Boolean abool) {
   containsValue_localInvalidate = abool;
}
public void containsKey_destroy(boolean abool) {
   containsKey_destroy = new Boolean(abool);
}
public void containsKey_destroy(Boolean abool) {
   containsKey_destroy = abool;
}
public void containsValue_destroy(boolean abool) {
   containsValue_destroy = new Boolean(abool);
}
public void containsValue_destroy(Boolean abool) {
   containsValue_destroy = abool;
}
public void containsKey_localDestroy(boolean abool) {
   containsKey_localDestroy = new Boolean(abool);
}
public void containsKey_localDestroy(Boolean abool) {
   containsKey_localDestroy = abool;
}
public void containsValue_localDestroy(boolean abool) {
   containsValue_localDestroy = new Boolean(abool);
}
public void containsValue_localDestroy(Boolean abool) {
   containsValue_localDestroy = abool;
}
public void containsKey_update(boolean abool) {
   containsKey_update = new Boolean(abool);
}
public void containsKey_update(Boolean abool) {
   containsKey_update = abool;
}
public void containsValue_update(boolean abool) {
   containsValue_update = new Boolean(abool);
}
public void containsValue_update(Boolean abool) {
   containsValue_update = abool;
}
public void containsKey_get(boolean abool) {
   containsKey_get = new Boolean(abool);
}
public void containsKey_get(Boolean abool) {
   containsKey_get = abool;
}
public void containsValue_get(boolean abool) {
   containsValue_get = new Boolean(abool);
}
public void containsValue_get(Boolean abool) {
   containsValue_get = abool;
}
public void containsKey_newKey(boolean abool) {
   containsKey_newKey = new Boolean(abool);
}
public void containsKey_newKey(Boolean abool) {
   containsKey_newKey = abool;
}
public void containsValue_newKey(boolean abool) {
   containsValue_newKey = new Boolean(abool);
}
public void containsValue_newKey(Boolean abool) {
   containsValue_newKey = abool;
}
public void exactSize(int anInt) {
   exactSize = new Integer(anInt);
}
public void exactSize(Integer anInt) {
   exactSize = anInt;
}
public void minSize(int anInt) {
   minSize = new Integer(anInt);
}
public void minSize(Integer anInt) {
   minSize = anInt;
}
public void maxSize(int anInt) {
   maxSize = new Integer(anInt);
}
public void maxSize(Integer anInt) {
   maxSize = anInt;
}
public void valueIsUpdated(boolean abool) {
   valueIsUpdated = abool;
}

//================================================================================
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(this.getClass().getName() + "\n");
   aStr.append("   containsKey_none: " + containsKey_none + "\n");
   aStr.append("   containsValue_none: " + containsValue_none + "\n");
   aStr.append("   containsKey_invalidate: " + containsKey_invalidate + "\n");
   aStr.append("   containsValue_invalidate: " + containsValue_invalidate + "\n");
   aStr.append("   containsKey_localInvalidate: " + containsKey_localInvalidate + "\n");
   aStr.append("   containsValue_localInvalidate: " + containsValue_localInvalidate + "\n");
   aStr.append("   containsKey_destroy: " + containsKey_destroy + "\n");
   aStr.append("   containsValue_destroy: " + containsValue_destroy + "\n");
   aStr.append("   containsKey_localDestroy: " + containsKey_localDestroy + "\n");
   aStr.append("   containsValue_localDestroy: " + containsValue_localDestroy + "\n");
   aStr.append("   containsKey_update: " + containsKey_update + "\n");
   aStr.append("   containsValue_update: " + containsValue_update + "\n");
   aStr.append("   containsKey_get: " + containsKey_get + "\n");
   aStr.append("   containsValue_get: " + containsValue_get + "\n");
   aStr.append("   containsKey_newKey: " + containsKey_newKey + "\n");
   aStr.append("   containsValue_newKey: " + containsValue_newKey + "\n");
   aStr.append("   exactSize: " + exactSize + "\n");
   aStr.append("   minSize: " + minSize + "\n");
   aStr.append("   maxSize: " + maxSize + "\n");
   return aStr.toString();
}

public Object clone() {
   try {
      return super.clone();
   } catch (CloneNotSupportedException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

}
