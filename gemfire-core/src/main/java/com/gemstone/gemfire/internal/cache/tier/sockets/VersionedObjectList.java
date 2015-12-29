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

package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.versions.DiskVersionTag;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * A refinement of ObjectPartList that adds per-entry versionTags and
 * has its own serialized form to allow key lists when there are no object components.
 * <P>
 * This class also implements Externalizable so that it can be serialized as
 * part of a PutAllPartialResultException.
 * 
 * @author Bruce Schuchardt
 * @since 7.0
 */

public class VersionedObjectList extends ObjectPartList implements Externalizable {

  public static final boolean DEBUG = Boolean.getBoolean("gemfire.VersionedObjectList.DEBUG");

  ArrayList<VersionTag> versionTags;
  
  private boolean regionIsVersioned;
  
  /** a flag telling us that after transmission of the list all objects should remain serialized */ 
  private boolean serializeValues;

  /**
   * add a new entry to the list
   * @param key the entry's key
   * @param versionTag the version tag for the entry, or null if there is no version information available
   */
  public void addKeyAndVersion(Object key, VersionTag versionTag) {
    if (DEBUG) {
      InternalDistributedSystem.getLoggerI18n().info(LocalizedStrings.DEBUG,
          "VersionedObjectList.addKeyAndVersion("+key+"; "+versionTag+")");
    }
    if (this.objects.size() > 0) {
      throw new IllegalStateException("attempt to add key/version to a list containing objects");
    }
    this.keys.add(key);
    if (this.regionIsVersioned) {
//      Assert.assertTrue(versionTag != null);
      this.versionTags.add(versionTag);
    }
  }
  
  @Override
  public void addPart(Object key, Object value, byte objectType, VersionTag versionTag) {
    if (DEBUG) {
      InternalDistributedSystem.getLoggerI18n().info(LocalizedStrings.DEBUG,
          "addPart("+key+"; "+value+"; "+objectType+"; "+versionTag /*+ "\n list=" + this*/);
    }
    super.addPart(key, value, objectType, versionTag);
    if (this.regionIsVersioned) { 
      int tagsSize = this.versionTags.size();
      if (keys != null && (tagsSize != this.keys.size()-1)) {
        // this should not happen - either all or none of the entries should have tags
        throw new InternalGemFireException();
      }
      if (this.objects != null && (this.objects.size() > 0) && (tagsSize != this.objects.size()-1)) {
        // this should not happen - either all or none of the entries should have tags
        throw new InternalGemFireException();
      }
//      Assert.assertTrue(versionTag != null);
      this.versionTags.add(versionTag);
    }
  }
  
  /**
   * add a versioned "map" entry to the list
   * 
   * @param key
   * @param value
   * @param versionTag
   */
  public void addObject(Object key, Object value, VersionTag versionTag) {
    addPart(key, value, OBJECT, versionTag);
  }

  // public methods

  public VersionedObjectList() {
    super();
    this.versionTags = new ArrayList();
  }

  public VersionedObjectList(boolean serializeValues) {
    this();
    this.serializeValues = serializeValues;
  }

  public VersionedObjectList(int maxSize, boolean hasKeys, boolean regionIsVersioned) {
    this(maxSize, hasKeys, regionIsVersioned, false);
  }
  
  public VersionedObjectList(int maxSize, boolean hasKeys, boolean regionIsVersioned, boolean serializeValues) {
    super(maxSize, hasKeys);
    if (regionIsVersioned) {
      this.versionTags = new ArrayList(maxSize);
      this.regionIsVersioned = true;
    } else {
      this.versionTags = new ArrayList();
    }
    this.serializeValues = serializeValues;
  }

  /**
   * replace null membership IDs in version tags with the given member ID.
   * VersionTags received from a server may have null IDs because they were
   * operations  performed by that server.  We transmit them as nulls to cut
   * costs, but have to do the swap on the receiving end (in the client)
   * @param sender
   */
  public void replaceNullIDs(DistributedMember sender) {
    for (VersionTag versionTag: versionTags) {
      if (versionTag != null) {
        versionTag.replaceNullIDs((InternalDistributedMember) sender);
      }
    }
  }

  @Override
  public void addObjectPartForAbsentKey(Object key, Object value) {
    addPart(key, value, KEY_NOT_AT_SERVER, null);
  }
  
  /**
   * Add a part for a destroyed or missing entry.  If the version tag is
   * not null this represents a tombstone.
   * @param key
   * @param value
   * @param version
   */
  public void addObjectPartForAbsentKey(Object key, Object value, VersionTag version) {
    addPart(key, value, KEY_NOT_AT_SERVER, version);
  }
  
  @Override
  public void addAll(ObjectPartList other) {
    if (DEBUG) {
      InternalDistributedSystem.getLoggerI18n().info(LocalizedStrings.DEBUG, "VOL.addAll(other="+other+"; this="+this/*, new Exception("stack trace")*/);
    }
    int myTypeArrayLength = this.hasKeys? this.keys.size() : this.objects.size();
    int otherTypeArrayLength = other.hasKeys? other.keys.size() : other.objects.size();
    super.addAll(other);
    VersionedObjectList vother = (VersionedObjectList)other;
    this.regionIsVersioned |= vother.regionIsVersioned;
    this.versionTags.addAll(vother.versionTags);
    if (myTypeArrayLength > 0 || otherTypeArrayLength > 0) {
      //    LogWriterI18n log = InternalDistributedSystem.getLoggerI18n();
      //    log.fine("this="+this+"\nother=" + other);
      int newSize = myTypeArrayLength + otherTypeArrayLength;
      if (this.objectTypeArray != null) {
        newSize = Math.max(newSize,this.objectTypeArray.length);
        if (this.objectTypeArray.length < newSize) { // need more room
          byte[] temp = this.objectTypeArray;
          this.objectTypeArray = new byte[newSize];
          System.arraycopy(temp, 0, this.objectTypeArray, 0, temp.length);
        }
      } else {
        this.objectTypeArray = new byte[newSize];
      }
      if (other.objectTypeArray != null) {
        System.arraycopy(other.objectTypeArray, 0, this.objectTypeArray, myTypeArrayLength, otherTypeArrayLength);
      }
    }
  }
  
  public List<VersionTag> getVersionTags() {
    return Collections.unmodifiableList(this.versionTags);
  }
  
  public boolean hasVersions() {
    return this.versionTags.size() > 0;
  }
  
  /** clear the version tags from this list */
  public void clearVersions() {
    this.versionTags = new ArrayList<VersionTag>(Math.max(50, this.size()));
  }
  
  /** 
   * Add a version, assuming that the key list has already been established and we are now
   * filling in version information.
   * @param tag the version tag to add
   */
  public void addVersion(VersionTag tag) {
    this.versionTags.add(tag);
  }
  
  /**
   * save the current key/tag pairs in the given map
   * @param vault
   */
  public void saveVersions(Map<Object, VersionTag> vault) {
    if (hasKeys) {
      int size = size();
      if (keys.size() < size) {
        size = keys.size();
      }
      java.util.Iterator<Object> keyIter = keys.iterator();
      java.util.Iterator<VersionTag> tagIter = versionTags.iterator();
      for (int i = 0; i < size; i++) {
        if (tagIter != null) {
          if (tagIter.hasNext()) {
            vault.put(keyIter.next(), tagIter.next());
            continue;
          }
          else {
            tagIter = null;
          }
        }
        vault.put(keyIter.next(), null);
      }
    }
  }

  /**
   * 
   * @return whether the source region had concurrency checks enabled
   */
  public boolean regionIsVersioned() {
    return this.regionIsVersioned;
  }
  
  /**
   * add some versionless keys
   * @param keySet
   */
  public void addAllKeys(Collection<Object> keySet) {
    if (!this.hasKeys) {
      this.hasKeys = true;
      this.keys = new ArrayList(keySet);
    } else {
      this.keys.addAll(keySet);
    }
  }

  @Override
  public void reinit(int maxSize) {
    super.reinit(maxSize);
    this.versionTags.clear();
  }
  
  /** sets the keys for this partlist */
  public VersionedObjectList setKeys(List newKeys) {
    this.keys = newKeys;
    this.hasKeys = (this.keys != null);
    return this;
  }
  
  public void clearObjects() {
    this.objects = Collections.emptyList();
    this.objectTypeArray = new byte[0];
  }

  @Override
  public void clear() {
    super.clear();
    this.versionTags.clear();
  }
  
  public void processVersionTags(InternalDistributedMember sender) {
    
  }
  
  static final byte FLAG_NULL_TAG = 0;
  static final byte FLAG_FULL_TAG = 1;
  static final byte FLAG_TAG_WITH_NEW_ID = 2;
  static final byte FLAG_TAG_WITH_NUMBER_ID = 3;

  @Override
  public void toData(DataOutput out) throws IOException {
    toData(out, 0, this.regionIsVersioned?this.versionTags.size():size(), true, true);
  }
  
  void toData(DataOutput out, int startIndex, int numEntries, boolean sendKeys,
      boolean sendObjects) throws IOException {
    LogWriterI18n log = null;
    if (DEBUG) {
      log = InternalDistributedSystem.getLoggerI18n();
    }
    int flags = 0;
    boolean hasObjects = false;
    boolean hasTags = false;
    if (sendKeys && this.hasKeys) {
      flags |= 0x01;
    }
    if (sendObjects && !this.objects.isEmpty()) {
      flags |= 0x02;
      hasObjects = true;
    }
    if (this.versionTags.size() > 0) {
      flags |= 0x04;
      hasTags = true;
      for (VersionTag tag : this.versionTags) {
        if (tag != null) {
          if (tag instanceof DiskVersionTag) {
            flags |= 0x20;
          }
          break;
        }
      }
    }
    if (this.regionIsVersioned) {
      flags |= 0x08;
    }
    if (this.serializeValues) {
      flags |= 0x10;
    }
    if (DEBUG) {
      log.info(LocalizedStrings.DEBUG, "serializing " + this + " with flags 0x" 
         + Integer.toHexString(flags) + " startIndex=" + startIndex
         + " numEntries=" + numEntries);
    }
    out.writeByte(flags);
    if (sendKeys && hasKeys) {
      int numToWrite = numEntries;
      if (numToWrite+startIndex > this.keys.size()) {
        numToWrite = Math.max(0, this.keys.size()-startIndex);
      }
      InternalDataSerializer.writeUnsignedVL(numToWrite, out);
      int index = startIndex;
      for (int i=0; i<numToWrite; i++, index++) {
        DataSerializer.writeObject(this.keys.get(index), out);
      }
    }
    if (sendObjects && hasObjects) {
      int numToWrite = numEntries;
      if (numToWrite+startIndex > this.objects.size()) {
        numToWrite = Math.max(0, this.objects.size()-startIndex);
      }
      InternalDataSerializer.writeUnsignedVL(numToWrite, out);
      int idx = 0;
      int index = startIndex;
      for (int i=0; i<numToWrite; i++, index++) {
        writeObject(this.objects.get(index), idx++, out, log);
      }
    }
    if (hasTags) {
      int numToWrite = numEntries;
      if (numToWrite+startIndex > this.versionTags.size()) {
        numToWrite = Math.max(0, this.versionTags.size()-startIndex);
      }
      InternalDataSerializer.writeUnsignedVL(numToWrite, out);
      Map<VersionSource, Integer> ids = new HashMap<VersionSource, Integer>(numToWrite);
      int idCount = 0;
      int index = startIndex;
      for (int i=0; i<numToWrite; i++, index++) {
        VersionTag tag = this.versionTags.get(index);
        if (tag == null) {
          out.writeByte(FLAG_NULL_TAG);
        } else {
          VersionSource id = tag.getMemberID();
          if (id == null) {
            out.writeByte(FLAG_FULL_TAG);
            InternalDataSerializer.invokeToData(tag, out);
          } else {
            Integer idNumber = ids.get(id);
            if (idNumber == null) {
              out.writeByte(FLAG_TAG_WITH_NEW_ID);
              idNumber = Integer.valueOf(idCount++);
              ids.put(id, idNumber);
              InternalDataSerializer.invokeToData(tag, out);
            } else {
              out.writeByte(FLAG_TAG_WITH_NUMBER_ID);
              tag.toData(out, false);
              tag.setMemberID(id);
              InternalDataSerializer.writeUnsignedVL(idNumber, out);
            }
          }
        }
      }
    }
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    LogWriterI18n log = null;
    if (DEBUG) {
      log = InternalDistributedSystem.getLoggerI18n();
    }
    int flags = in.readByte();
    this.hasKeys = (flags & 0x01) == 0x01;
    boolean hasObjects = (flags & 0x02) == 0x02;
    boolean hasTags = (flags & 0x04) == 0x04;
    this.regionIsVersioned = (flags & 0x08) == 0x08;
    this.serializeValues = (flags & 0x10) == 0x10;
    boolean persistent= (flags & 0x20) == 0x20;
    if (DEBUG) {
      log.info(LocalizedStrings.DEBUG, "deserializing a VersionedObjectList with flags 0x" + Integer.toHexString(flags));
    }
    if (this.hasKeys) {
      int size = (int)InternalDataSerializer.readUnsignedVL(in);
      this.keys = new ArrayList(size);
      if (DEBUG) {
        log.info(LocalizedStrings.DEBUG, "reading " + size + " keys");
      }
      for (int i=0; i<size; i++) {
        this.keys.add(DataSerializer.readObject(in));
      }
    }
    if (hasObjects) {
      int size = (int)InternalDataSerializer.readUnsignedVL(in);
      if (DEBUG) {
        log.info(LocalizedStrings.DEBUG, "reading " + size + " objects");
      }
      this.objects = new ArrayList(size);
      this.objectTypeArray = new byte[size];
      for (int i=0; i<size; i++) {
        readObject(i, in, log);
      }
    } else {
      this.objects = new ArrayList();
    }
    if (hasTags) {
      int size = (int)InternalDataSerializer.readUnsignedVL(in);
      if (DEBUG) {
        log.info(LocalizedStrings.DEBUG, "reading " + size + " version tags");
      }
      this.versionTags = new ArrayList<VersionTag>(size);
      List<VersionSource> ids = new ArrayList<VersionSource>(size);
      for (int i=0; i<size; i++) {
        byte entryType = in.readByte();
        switch (entryType) {
        case FLAG_NULL_TAG:
          this.versionTags.add(null);
          break;
        case FLAG_FULL_TAG:
          this.versionTags.add(VersionTag.create(persistent, in));
          break;
        case FLAG_TAG_WITH_NEW_ID:
          VersionTag tag = VersionTag.create(persistent, in);
          ids.add(tag.getMemberID());
          this.versionTags.add(tag);
          break;
        case FLAG_TAG_WITH_NUMBER_ID:
          tag = VersionTag.create(persistent, in);
          int idNumber = (int)InternalDataSerializer.readUnsignedVL(in);
          tag.setMemberID(ids.get(idNumber));
          this.versionTags.add(tag);
          break;
        }
      }
    } else {
      this.versionTags = new ArrayList<VersionTag>();
    }
  }
  
  private void writeObject(Object value, int index, DataOutput out, LogWriterI18n log) throws IOException {
    byte objectType = this.objectTypeArray[index];
    if (DEBUG) {
      log.info(LocalizedStrings.DEBUG, "writing object " + index + " of type " + objectType + ": " + value);
    }
    out.writeByte(objectType);
    if (objectType == OBJECT && value instanceof byte[]) {
      if (this.serializeValues) {
        DataSerializer.writeByteArray((byte[])value, out);
      } else {
        out.write((byte[])value);
      }
    }
    else if (objectType == EXCEPTION) {
      // write exception as byte array so native clients can skip it
      DataSerializer
          .writeByteArray(CacheServerHelper.serialize(value), out);
      // write the exception string for native clients
      DataSerializer.writeString(value.toString(), out);
    }
    else {
      if (this.serializeValues) {
        DataSerializer.writeObjectAsByteArray(value, out);
      } else {
        DataSerializer.writeObject(value, out);
      }
    }
  }


  private void readObject(int index, DataInput in, LogWriterI18n log) throws IOException, ClassNotFoundException {
    Object value;
    this.objectTypeArray[index] = in.readByte();
    if (DEBUG) {
      log.info(LocalizedStrings.DEBUG, "reading object " + index + " of type " + objectTypeArray[index]);
    }
    boolean isException = this.objectTypeArray[index] == EXCEPTION;
    if (isException) {
      byte[] exBytes = DataSerializer.readByteArray(in);
      value = CacheServerHelper.deserialize(exBytes);
      // ignore the exception string meant for native clients
      DataSerializer.readString(in);
    }
    else if (this.serializeValues) {
      value = DataSerializer.readByteArray(in);
    } else {
      value = DataSerializer.readObject(in);
    }
    this.objects.add(value);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    toData(out);
  }
  
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    fromData(in);
  }
  
  @Override
  public int getDSFID() {
    return DataSerializableFixedID.VERSIONED_OBJECT_LIST;
  }
  
  @Override
  public String toString() {
    StringBuilder desc = new StringBuilder();
    desc.append("VersionedObjectList(regionVersioned=").append(regionIsVersioned)
      .append("; hasKeys=").append(hasKeys)
      .append("; keys=")
      .append(keys==null? "null" : String.valueOf(keys.size()))
      .append("; objects=").append(objects.size())
      .append("; isobject=").append(objectTypeArray==null? "null" : objectTypeArray.length)
      .append("; ccEnabled=").append(this.regionIsVersioned)
      .append("; versionTags=").append(String.valueOf(versionTags.size()))
      .append(")\n");
    Iterator entries = this.iterator();
    while (entries.hasNext()) {
      desc.append(entries.next()).append("\n");
    }
    return desc.toString();
  }
  
  public Set keySet() {
    if (!this.hasKeys) {
      return Collections.EMPTY_SET;
    } else {
      return new HashSet(this.keys);
    }
  }

  public void setRegionContext(LocalRegion region) {
    if (this.hasKeys && region.keyRequiresRegionContext()) {
      for (Object k : this.keys) {
        ((KeyWithRegionContext)k).setRegionContext(region);
      }
    }
  }

  public class Entry implements Map.Entry {
    int index;
    
    Entry(int idx) {
      this.index = idx;
    }
    
    public Object getKey() {
      if (hasKeys) {
        return keys.get(index);
      }
      return null;
    }
    public Object getObject() {
      return objects.get(index);
    }
    public VersionTag getVersionTag() {
      if (index < versionTags.size()) {
        return versionTags.get(index);
      }
      return null;
    }
    
    public byte getObjectType() {
      return objectTypeArray[index];
    }
    
    public boolean isKeyNotOnServer() {
      return objectTypeArray[index] == KEY_NOT_AT_SERVER;
    }

    public boolean isBytes() {
      return objectTypeArray[index] == BYTES;
    }
    
    public boolean isException() {
      return objectTypeArray[index] == EXCEPTION;
    }

    /* (non-Javadoc)
     * @see java.util.Map.Entry#getValue()
     */
    public Object getValue() {
      if (index < objects.size()) {
        return objects.get(index);
      } else {
        return null;
      }
    }

    /* (non-Javadoc)
     * @see java.util.Map.Entry#setValue(java.lang.Object)
     */
    public Object setValue(Object value) {
      Object result = objects.get(index);
      objects.set(index, value);
      return result;
    }
    
    @Override
    public String toString() {
      return "[key="+getKey()+",value="+getValue()+",v="+getVersionTag()+"]";
    }
    
  }
  
  public Iterator iterator() {
    return new Iterator();
  }
  
  public class Iterator implements java.util.Iterator {
    int index = 0;
    int size = size();
    
    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext() {
      return index < size;
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public Entry next() {
      return new Entry(index++);
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#remove()
     */
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }
  
  /**
   * Chunker is used to send a VersionedObjectList to a client in pieces.
   * It works by pretending to be a VersionedObjectList during serialization
   * and writing only a portion of the list in each invocation of toData().
   * 
   * @author bruces
   *
   */
  public static class Chunker implements DataSerializableFixedID {
    private int index = 0;
    private VersionedObjectList list;
    private int chunkSize;
    private boolean sendKeys;
    private boolean sendObjects;
    
    public Chunker(VersionedObjectList list, int chunkSize, boolean sendKeys, boolean sendObjects) {
      this.list = list;
      this.chunkSize = chunkSize;
      this.sendKeys = sendKeys;
      this.sendObjects = sendObjects;
    }
    
    @Override
    public int getDSFID() {
      return DataSerializableFixedID.VERSIONED_OBJECT_LIST;
    }
    
    @Override
    public void toData(DataOutput out) throws IOException {
      int startIndex = index;
      this.index += this.chunkSize;
      this.list.toData(out, startIndex, chunkSize, sendKeys, sendObjects);
    }
    
    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      throw new IOException("this fromData method should never be invoked");
    }
    
    @Override
    public Version[] getSerializationVersions() {
      return this.list.getSerializationVersions();
    }
  }

}
