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
package com.gemstone.gemfire;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.WritablePdxInstance;
import com.gemstone.gemfire.cache.util.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.*;

/**
 * Tests the functionality of the {@link CopyHelper#copy} method
 * and the builtin copy-on-get Cache functions.
 *
 * @author Darrel Schneider
 * @since 4.0
 *
 */
public class CopyTest extends TestCase {

  private Cache cache;
  private Region region;

  protected Object oldValue;
  protected Object newValue;
  
  public CopyTest(String name) {
    super(name);
  }

  ////////  Test methods


  private void createCache(boolean copyOnRead) throws CacheException {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0"); // loner
    this.cache = CacheFactory.create(DistributedSystem.connect(p));
    this.cache.setCopyOnRead(copyOnRead);

    AttributesFactory af = new AttributesFactory();
    af.setScope(Scope.LOCAL);
    af.setCacheListener(new CacheListenerAdapter() {
        @Override
        public void afterCreate(EntryEvent event) {
          oldValue = event.getOldValue();
          newValue = event.getNewValue();
        }
        @Override
        public void afterUpdate(EntryEvent event) {
          oldValue = event.getOldValue();
          newValue = event.getNewValue();
        }
        @Override
        public void afterInvalidate(EntryEvent event) {
          oldValue = event.getOldValue();
          newValue = event.getNewValue();
        }
        @Override
        public void afterDestroy(EntryEvent event) {
          oldValue = event.getOldValue();
          newValue = event.getNewValue();
        }
        @Override
        public void afterRegionInvalidate(RegionEvent event) {
          // ignore
        }
        @Override
        public void afterRegionDestroy(RegionEvent event) {
          // ignore
        }
        @Override
        public void close() {
          oldValue = null;
          newValue = null;
        }
      });
    this.region = this.cache.createRegion("CopyTest", af.create());
  }
  private void closeCache() {
    if (this.cache != null) {
      this.region = null;
      Cache c = this.cache;
      this.cache = null;
      c.close();
    }
  }
  
  public void testSimpleCopies() {
    assertTrue(null == CopyHelper.copy(null));
    try {
      CopyHelper.copy(new Object());
      fail("Expected CopyException");
    } catch (CopyException ok) {
    }
    CopyHelper.copy(new CloneImpl());
  }
  protected static class CloneImpl implements Cloneable {
    @Override
    public Object clone() {
      return this;
    }
  }
  
  public void testReferences() throws Exception {
    createCache(false);
    try {
      final Object ov = new Integer(6);
      final Object v = new Integer(7);
      this.region.put("key", ov);
      this.region.put("key", v);
      assertTrue("expected listener getOldValue to return reference to ov", this.oldValue == ov);
      assertTrue("expected listener getNewValue to return reference to v", this.newValue == v);
      assertTrue("expected get to return reference to v", this.region.get("key") == v);
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return reference to v", re.getValue() == v);
      Collection c = this.region.values();
      Object[] cArray = c.toArray();
      assertTrue("expected values().toArray() to return reference to v", cArray[0] == v);
      assertTrue("expected values().iterator().next() to return reference to v", c.iterator().next() == v);
    } finally {
      closeCache();
    }
  }
  
  public static class ModifiableInteger implements Serializable {
    private static final long serialVersionUID = 9085003409748155613L;
    private final int v;
    public ModifiableInteger(int v) {
      this.v = v;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + v;
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ModifiableInteger other = (ModifiableInteger) obj;
      if (v != other.v)
        return false;
      return true;
    }
  }

  public void testCopies() throws Exception {
    createCache(true);
    try {
      final Object ov = new ModifiableInteger(1);
      final Object v = new ModifiableInteger(2);
      this.region.put("key", ov);
      this.region.put("key", v);
      assertTrue("expected listener getOldValue to return copy of ov", this.oldValue != ov);
      assertEquals(ov, this.oldValue);
      assertTrue("expected listener getNewValue to return copy of v", this.newValue != v);
      assertEquals(v, this.newValue);
      assertTrue("expected get to return copy of v", this.region.get("key") != v);
      assertEquals(v, this.region.get("key"));
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return copy of v", re.getValue() != v);
      assertEquals(v, re.getValue());
      Collection c = this.region.values();
      Object[] cArray = c.toArray();
      assertTrue("expected values().toArray() to return copy of v", cArray[0] != v);
      assertEquals(v, cArray[0]);
      
      assertTrue("expected values().iterator().next() to return copy of v", c.iterator().next() != v);
      assertEquals(v, c.iterator().next());
    } finally {
      closeCache();
    }
  }
  public void testImmutable() throws Exception {
    createCache(true);
    try {
      // Integer is immutable so copies should not be made
      final Object ov = new Integer(6);
      final Object v = new Integer(7);
      this.region.put("key", ov);
      this.region.put("key", v);
      assertSame(ov, this.oldValue);
      assertSame(v, this.newValue);
      assertSame(v, this.region.get("key"));
      Region.Entry re = this.region.getEntry("key");
      assertSame(v, re.getValue());
      Collection c = this.region.values();
      Object[] cArray = c.toArray();
      assertSame(v, cArray[0]);
      
      assertSame(v, c.iterator().next());
    } finally {
      closeCache();
    }
  }
  
  public void testPrimitiveArrays() {
    {
      byte[] ba1 = new byte[]{1,2,3};
      byte[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      boolean[] ba1 = new boolean[]{true, false, true};
      boolean[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      char[] ba1 = new char[]{1,2,3};
      char[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      short[] ba1 = new short[]{1,2,3};
      short[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      int[] ba1 = new int[]{1,2,3};
      int[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      long[] ba1 = new long[]{1,2,3};
      long[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      float[] ba1 = new float[]{1,2,3};
      float[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
    {
      double[] ba1 = new double[]{1,2,3};
      double[] ba2 = CopyHelper.copy(ba1);
      if (ba1 == ba2) {
        fail("expected new instance of primitive array");
      }
      if (!Arrays.equals(ba1, ba2)) {
        fail("expected contents of arrays to be equal");
      }
    }
  }
  public void testObjectArray() {
    Object[] oa1 = new Object[]{1,2,3};
    Object[] oa2 = CopyHelper.copy(oa1);
    if (oa1 == oa2) {
      fail("expected new instance of object array");
    }
    if (!Arrays.equals(oa1, oa2)) {
      fail("expected contents of arrays to be equal");
    }
  }
  
  public void testIsWellKnownImmutableInstance() {
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance("abc"));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Integer.valueOf(0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Long.valueOf(0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Byte.valueOf((byte) 0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Short.valueOf((short) 0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Float.valueOf((float) 1.2)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Double.valueOf(1.2)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(Character.valueOf((char) 0)));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(new BigInteger("1234")));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(new BigDecimal("123.4556")));
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(new UUID(1L, 2L)));
    PdxInstance pi = new PdxInstance() {
      public Object getObject() {
        return null;
      }
      public boolean hasField(String fieldName) {
        return false;
      }
      public List<String> getFieldNames() {
        return null;
      }
      public boolean isIdentityField(String fieldName) {
        return false;
      }
      public Object getField(String fieldName) {
        return null;
      }
      public WritablePdxInstance createWriter() {
        return null;
      }
      public String getClassName() {
        return null;
      }
      public boolean isEnum() {
        return false;
      }
    };
    WritablePdxInstance wpi = new WritablePdxInstance() {
      public Object getObject() {
        return null;
      }
      public boolean hasField(String fieldName) {
        return false;
      }
      public List<String> getFieldNames() {
        return null;
      }
      public boolean isIdentityField(String fieldName) {
        return false;
      }
      public Object getField(String fieldName) {
        return null;
      }
      public WritablePdxInstance createWriter() {
        return null;
      }
      public void setField(String fieldName, Object value) {
      }
      public String getClassName() {
        return null;
      }
      public boolean isEnum() {
        return false;
      }
    };
    assertEquals(true, CopyHelper.isWellKnownImmutableInstance(pi));
    assertEquals(false, CopyHelper.isWellKnownImmutableInstance(wpi));
    assertEquals(false, CopyHelper.isWellKnownImmutableInstance(new Object()));
  }
/*
 *       if (o instanceof Integer) return true;
      if (o instanceof Long) return true;
      if (o instanceof Byte) return true;
      if (o instanceof Short) return true;
      if (o instanceof Float) return true;
      if (o instanceof Double) return true;
      // subclasses of non-final classes may be mutable
      if (o.getClass().equals(BigInteger.class)) return true;
      if (o.getClass().equals(BigDecimal.class)) return true;
    }
    if (o instanceof PdxInstance && !(o instanceof WritablePdxInstance)) {
      // no need to copy since it is immutable
      return true;
    }
    if (o instanceof Character) return true;

 */
  public void testTxReferences() throws Exception {
    createCache(false);
    final CacheTransactionManager txMgr = this.cache.getCacheTransactionManager();
    txMgr.begin();
    try {
      final Object v = new Integer(7);
      this.region.put("key", v);
      assertTrue("expected get to return reference to v", this.region.get("key") == v);
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return reference to v", re.getValue() == v);
      txMgr.rollback();
    } finally {
      try {
        txMgr.rollback();
      } catch (IllegalTransactionStateException ignore) {
      }
      closeCache();
    }
  }

  public void testTxCopies() throws Exception {
    createCache(true);
    final CacheTransactionManager txMgr = this.cache.getCacheTransactionManager();
    txMgr.begin();
    try {
      final Object v = new ModifiableInteger(7);
      this.region.put("key", v);
      assertTrue("expected get to return copy of v", this.region.get("key") != v);
      assertEquals(v, this.region.get("key"));
      Region.Entry re = this.region.getEntry("key");
      assertTrue("expected Entry.getValue to return copy of v", re.getValue() != v);
      assertEquals(v, re.getValue());
      txMgr.rollback();
    } finally {
      try {
        txMgr.rollback();
      } catch (IllegalTransactionStateException ignore) {
      }
      closeCache();
    }
  }

}
