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
package com.gemstone.gemfire.internal.offheap;

import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteOrder;

import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.DiskEntry;
import com.gemstone.gemfire.internal.cache.DiskId;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.OffHeapRegionEntry;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.Chunk;
import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl.DataAsAddress;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.gemstone.gemfire.pdx.internal.unsafe.UnsafeWrapper;

/**
 * The class just has static methods
 * that operate on instances of {@link OffHeapRegionEntry}.
 * It allows common code to be shared for all the
 * classes we have that implement {@link OffHeapRegionEntry}.
 * 
 * @author darrel
 *
 */
public class OffHeapRegionEntryHelper {

  public static final long NULL_ADDRESS = 0L<<1;
  private static final long INVALID_ADDRESS = 1L<<1;
  private static final long LOCAL_INVALID_ADDRESS = 2L<<1;
  private static final long DESTROYED_ADDRESS = 3L<<1;
  public static final long REMOVED_PHASE1_ADDRESS = 4L<<1;
  private static final long REMOVED_PHASE2_ADDRESS = 5L<<1;
  private static final long END_OF_STREAM_ADDRESS = 6L<<1;
  private static final long NOT_AVAILABLE_ADDRESS = 7L<<1;
  private static final long TOMBSTONE_ADDRESS = 8L<<1;
  public static final int MAX_LENGTH_FOR_DATA_AS_ADDRESS = 8;
 /* private static final ChunkFactory chunkFactory ;
  static {
    ChunkFactory factory;
    try {
       factory= SimpleMemoryAllocatorImpl.getAllocator().getChunkFactory();
         
    }catch(CacheClosedException ce) {
      factory = null;
    }
    chunkFactory = factory;
  }*/
  
  private static final Token[] addrToObj = new Token[]{
    null,
    Token.INVALID,
    Token.LOCAL_INVALID,
    Token.DESTROYED,
    Token.REMOVED_PHASE1,
    Token.REMOVED_PHASE2,
    Token.END_OF_STREAM,
    Token.NOT_AVAILABLE,
    Token.TOMBSTONE,
  };
  
  private static long objectToAddress(@Unretained Object v) {
    if (v instanceof Chunk) return ((Chunk) v).getMemoryAddress();
    if (v instanceof DataAsAddress) return ((DataAsAddress) v).getEncodedAddress();
    if (v == null) return NULL_ADDRESS;
    if (v == Token.TOMBSTONE) return TOMBSTONE_ADDRESS;
    if (v == Token.INVALID) return INVALID_ADDRESS;
    if (v == Token.LOCAL_INVALID) return LOCAL_INVALID_ADDRESS;
    if (v == Token.DESTROYED) return DESTROYED_ADDRESS;
    if (v == Token.REMOVED_PHASE1) return REMOVED_PHASE1_ADDRESS;
    if (v == Token.REMOVED_PHASE2) return REMOVED_PHASE2_ADDRESS;
    if (v == Token.END_OF_STREAM) return END_OF_STREAM_ADDRESS;
    if (v == Token.NOT_AVAILABLE) return NOT_AVAILABLE_ADDRESS;
    throw new IllegalStateException("Can not convert " + v + " to an off heap address.");
  }

  static Object encodedAddressToObject(long ohAddress) {
    return encodedAddressToObject(ohAddress, true, true);
  }
  
  //TODO:Asif:Check if this is a valid equality conditions
  public static boolean isAddressInvalidOrRemoved(long address) {
    return address == INVALID_ADDRESS || address == LOCAL_INVALID_ADDRESS 
        || address == REMOVED_PHASE2_ADDRESS || address == NULL_ADDRESS; 
  }
  
  /**
   * This method may Release the object stored at ohAddres if the object is 
   * also decompressed into another off-heap location.  This decompressed
   * object will be Retained and returned, or an object created from the
   * original address may be returned as Unretained.
   * 
   * @param ohAddress OFF_HEAP_ADDRESS
   * @param decompress true if off-heap value should be decompressed before returning
   * @return OFF_HEAP_OBJECT (sometimes)
   */
  @Unretained @Retained
  public static Object addressToObject(@Released @Retained long ohAddress, boolean decompress) {
    if (isOffHeap(ohAddress)) {
      //Chunk chunk = chunkFactory.newChunk(ohAddress);
      @Unretained Chunk chunk =  SimpleMemoryAllocatorImpl.getAllocator().getChunkFactory().newChunk(ohAddress);
      @Unretained Object result = chunk;
      if (decompress && chunk.isCompressed()) {
        try {
          // to fix bug 47982 need to:
          if (chunk.isSerialized()
              && !CachedDeserializableFactory.preferObject()) {
            // return a VMCachedDeserializable with the decompressed serialized bytes since chunk is serialized
            result = CachedDeserializableFactory.create(chunk.getSerializedValue());
          } else {
            // return a byte[] since chunk is not serialized
            result = chunk.getDeserializedForReading();
          }
        } finally {
          // decompress is only true when this method is called by _getValueRetain.
          // In that case the caller has already retained ohAddress because it thought
          // we would return it. But we have unwrapped it and are returning the decompressed results.
          // So we need to release the chunk here.
            chunk.release();
        }
      }
      return result;
    } else if ((ohAddress & ENCODED_BIT) != 0) {
      return new DataAsAddress(ohAddress); // TODO does the decompress flag matter here?
    } else {
      return addrToObj[(int) ohAddress>>1];
    }
  }
  
  public static int getSerializedLengthFromDataAsAddress(DataAsAddress dataAsAddress) {
    final long ohAddress = dataAsAddress.getEncodedAddress();
    
     if ((ohAddress & ENCODED_BIT) != 0) {     
      boolean isLong = (ohAddress & LONG_BIT) != 0;     
      if (isLong) {
       return 9;
      } else {
        return (int) ((ohAddress & SIZE_MASK) >> SIZE_SHIFT);        
      }     
    } else {
      return 0;
    }
  }
  
  
  private static Token addressToToken(long ohAddress) {
    if (isOffHeap(ohAddress) || (ohAddress & ENCODED_BIT) != 0) {
      return Token.NOT_A_TOKEN;
    } else {
      return addrToObj[(int) ohAddress>>1];
    }
  }

  private static void releaseAddress(@Released long ohAddress) {
    if (isOffHeap(ohAddress)) {
      Chunk.release(ohAddress, true);
    }
  }
  
  /**
   * The address in 're' will be @Released.
   */
  public static void releaseEntry(@Released OffHeapRegionEntry re) {
    if (re instanceof DiskEntry) {
      DiskId did = ((DiskEntry) re).getDiskId();
      if (did != null && did.isPendingAsync()) {
        synchronized (did) {
          // This may not be needed so remove this call if it causes problems.
          // We no longer need this entry to be written to disk so unschedule it
          // before we change its value to REMOVED_PHASE2.
          did.setPendingAsync(false);
          setValue(re, Token.REMOVED_PHASE2);
          return;
        }
      }
    }
    setValue(re, Token.REMOVED_PHASE2);
  }

  public static void releaseEntry(@Unretained OffHeapRegionEntry re, @Released MemoryChunkWithRefCount expectedValue) {
    long oldAddress = objectToAddress(expectedValue);
    final long newAddress = objectToAddress(Token.REMOVED_PHASE2);
    if (re.setAddress(oldAddress, newAddress) || re.getAddress() != newAddress) {
      releaseAddress(oldAddress);
    } /*else {
      if (!calledSetValue || re.getAddress() != newAddress) {
        expectedValue.release();
      }
    }*/
  }
  
  /**
   * This bit is set to indicate that this address has data encoded in it.
   */
  private static final long ENCODED_BIT = 1L;
  /**
   * This bit is set to indicate that the encoded data is serialized.
   */
  private static final long SERIALIZED_BIT = 2L;
  /**
   * This bit is set to indicate that the encoded data is compressed.
   */
  private static final long COMPRESSED_BIT = 4L;
  /**
   * This bit is set to indicate that the encoded data is a long whose value fits in 7 bytes.
   */
  private static final long LONG_BIT = 8L;
  /**
   * size is in the range 0..7 so we only need 3 bits.
   */
  private static final long SIZE_MASK = 0x70L;
  /**
   * number of bits to shift the size by.
   */
  private static final int SIZE_SHIFT = 4;
  // the msb of this byte is currently unused

  public static final boolean NATIVE_BYTE_ORDER_IS_LITTLE_ENDIAN =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  /**
   * Returns 0 if the data could not be encoded as an address.
   */
  public static long encodeDataAsAddress(byte[] v, boolean isSerialized, boolean isCompressed) {
    if (v.length < MAX_LENGTH_FOR_DATA_AS_ADDRESS) {
      long result = 0L;
      for (int i=0; i < v.length; i++) {
        result |= v[i] & 0x00ff;
        result <<= 8;
      }
      result |= (v.length << SIZE_SHIFT) | ENCODED_BIT;
      if (isSerialized) {
        result |= SERIALIZED_BIT;
      }
      if (isCompressed) {
        result |= COMPRESSED_BIT;
      }
      return result;
    } else if (isSerialized && !isCompressed) {
      // Check for some special types that take more than 7 bytes to serialize
      // but that might be able to be inlined with less than 8 bytes.
      if (v[0] == DSCODE.LONG) {
        // A long is currently always serialized as 8 bytes (9 if you include the dscode).
        // But many long values will actually be small enough for is to encode in 7 bytes.
        if ((v[1] == 0 && (v[2] & 0x80) == 0) || (v[1] == -1 && (v[2] & 0x80) != 0)) {
          // The long can be encoded as 7 bytes since the most signification byte
          // is simply an extension of the sign byte on the second most signification byte.
          long result = 0L;
          for (int i=2; i < v.length; i++) {
            result |= v[i] & 0x00ff;
            result <<= 8;
          }
          result |= (7 << SIZE_SHIFT) | LONG_BIT | SERIALIZED_BIT | ENCODED_BIT;
          return result;
        }
      }
    }
    return 0L;
  }
  
  public static Object encodedAddressToObject(long addr, boolean decompress, boolean deserialize) {
    boolean isSerialized = (addr & SERIALIZED_BIT) != 0;
    byte[] bytes = encodedAddressToBytes(addr, decompress);
    if (isSerialized) {
      if (deserialize || CachedDeserializableFactory.preferObject()) {
        return EntryEventImpl.deserialize(bytes);
      } else {
        return CachedDeserializableFactory.create(bytes);
      }
    } else {
      return bytes;
    }
  }

  public static byte[] encodedAddressToExpectedRawBytes(final long addr,
      final boolean decompress) {
    final byte[] bytes = encodedAddressToBytes(addr, decompress);
    if ((addr & SERIALIZED_BIT) == 0) {
      return bytes;
    }
    else {
      Object obj = EntryEventImpl.deserialize(bytes);
      if (Token.isInvalidOrRemoved(obj) || obj == Token.NOT_AVAILABLE) {
        return null;
      }
      else {
        return (byte[])obj;
      }
    }
  }

  static byte[] encodedAddressToBytes(long addr) {
    byte[] result = encodedAddressToBytes(addr, true);
    boolean isSerialized = (addr & SERIALIZED_BIT) != 0;
    if (!isSerialized) {
      result = EntryEventImpl.serialize(result);
    }
    return result;
  }
  
  public static int getDataSizeFromEncodedAddress(long addr) {
    final int size = (int) ((addr & SIZE_MASK) >> SIZE_SHIFT);
    return size;
  }

  private static byte[] encodedAddressToBytes(long addr, boolean decompress) {
    assert (addr & ENCODED_BIT) != 0;
    boolean isCompressed = (addr & COMPRESSED_BIT) != 0;
    int size = (int) ((addr & SIZE_MASK) >> SIZE_SHIFT);
    boolean isLong = (addr & LONG_BIT) != 0;
    byte[] bytes;
    if (isLong) {
      bytes = new byte[9];
      bytes[0] = DSCODE.LONG;
      for (int i=8; i >=2; i--) {
        addr >>= 8;
        bytes[i] = (byte) (addr & 0x00ff);
      }
      if ((bytes[2] & 0x80) != 0) {
        bytes[1] = -1;
      } else {
        bytes[1] = 0;
      }
    } else {
      bytes = new byte[size];
      for (int i=size-1; i >=0; i--) {
        addr >>= 8;
        bytes[i] = (byte) (addr & 0x00ff);
      }
    }
    if (decompress && isCompressed) {
      bytes = SimpleMemoryAllocatorImpl.getAllocator().getCompressor().decompress(bytes);
    }
    return bytes;
  }

  /**
   * The previous value at the address in 're' will be @Released and then the
   * address in 're' will be set to the @Unretained address of 'v'.
   */
  public static void setValue(@Released OffHeapRegionEntry re, @Unretained Object v) {
    // setValue is called when synced so I don't need to worry
    // about oldAddress being released by someone else.
    final long newAddress = objectToAddress(v);
    long oldAddress;
    do {
      oldAddress = re.getAddress();
    } while (!re.setAddress(oldAddress, newAddress));
    SimpleMemoryAllocatorImpl.setReferenceCountOwner(re);
    releaseAddress(oldAddress);
    SimpleMemoryAllocatorImpl.setReferenceCountOwner(null);
  }
 
  public static Token getValueAsToken(@Unretained OffHeapRegionEntry re) {
    return addressToToken(re.getAddress());
  }

  @Unretained
  public static Object _getValue(@Unretained OffHeapRegionEntry re) {
    return addressToObject(re.getAddress(), false);
  }
  
  public static boolean isOffHeap(long addr) {
    if ((addr & ENCODED_BIT) != 0) return false;
    if (addr < 0) return true;
    addr >>= 1; // shift left 1 to convert to array index;
    return addr >= addrToObj.length;
  }

  /**
   * If the value stored at the location held in 're' is returned, then it will
   * be Retained.  If the value returned is 're' decompressed into another
   * off-heap location, then 're' will be Unretained but the new,
   * decompressed value will be Retained.  Therefore, whichever is returned
   * (the value at the address in 're' or the decompressed value) it will have
   * been Retained.
   * 
   * @return possible OFF_HEAP_OBJECT (caller must release)
   */
  @Retained
  public static Object _getValueRetain(@Retained @Unretained OffHeapRegionEntry re, boolean decompress) {
    int retryCount = 0;
    @Retained long addr = re.getAddress();
    while (isOffHeap(addr)) {
      if (Chunk.retain(addr)) {
        @Unretained long addr2 = re.getAddress();
        if (addr != addr2) {
          retryCount = 0;
          Chunk.release(addr, true);
          // spin around and try again.
          addr = addr2;
        } else {
          return addressToObject(addr, decompress);
        }
      } else {
        // spin around and try again
        long addr2 = re.getAddress();
        retryCount++;
        if (retryCount > 100) {
          throw new IllegalStateException("retain failed addr=" + addr + " addr2=" + addr + " 100 times" + " history=" + SimpleMemoryAllocatorImpl.getFreeRefCountInfo(addr));
        }
        addr = addr2;
        // Since retain returned false our region entry should have a different
        // value in it. However the actual address could be the exact same one
        // because addr was released, then reallocated from the free list and set
        // back into this region entry. See bug 47782
      }
    }
    return addressToObject(addr, decompress);
  }
  
 

  public static boolean isSerialized(long address) {
    return (address & SERIALIZED_BIT) != 0;
  }

  public static boolean isCompressed(long address) {
    return (address & COMPRESSED_BIT) != 0;
  }

  /**
   * Optimized method to copy raw bytes from off-heap memory to DataOutput
   * without creating an intermediate heap byte[]. Note: for unbuffered
   * DataOutput streams, one is better off using intermediate byte[] otherwise
   * multiple calls into DataOutput can be inefficient.
   */
  public static void copyBytesToDataOutput(final UnsafeWrapper unsafe,
      long memOffset, int length, final DataOutput out) throws IOException {
    // copying long at a time gives best performance but we need to take
    // care of endianness
    if (NATIVE_BYTE_ORDER_IS_LITTLE_ENDIAN) {
      while (length >= 8) {
        out.writeLong(Long.reverseBytes(unsafe.getLong(memOffset)));
        memOffset += 8;
        length -= 8;
      }
      if (length >= 4) {
        // copy using int from remaining length
        out.writeInt(Integer.reverseBytes(unsafe.getInt(memOffset)));
        memOffset += 4;
        length -= 4;
      }
      while (length > 0) {
        // copy rest a byte at a time
        out.write(unsafe.getByte(memOffset));
        memOffset++;
        length--;
      }
    }
    else {
      while (length >= 8) {
        out.writeLong(unsafe.getLong(memOffset));
        memOffset += 8;
        length -= 8;
      }
      if (length >= 4) {
        // copy using int from remaining length
        out.writeInt(unsafe.getInt(memOffset));
        memOffset += 4;
        length -= 4;
      }
      while (length > 0) {
        // copy rest a byte at a time
        out.write(unsafe.getByte(memOffset));
        memOffset++;
        length--;
      }
    }
  }

  private static final ThreadLocal<Object> clearNeedsToCheckForOffHeap = new ThreadLocal<Object>();
  public static boolean doesClearNeedToCheckForOffHeap() {
    return clearNeedsToCheckForOffHeap.get() != null;
  }
  public static void doWithOffHeapClear(Runnable r) {
    clearNeedsToCheckForOffHeap.set(Boolean.TRUE);
    try {
      r.run();
    } finally {
      clearNeedsToCheckForOffHeap.remove();
    }
  }
}
