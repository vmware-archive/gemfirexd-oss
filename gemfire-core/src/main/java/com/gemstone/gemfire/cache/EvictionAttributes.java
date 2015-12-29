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

package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.control.ResourceManager;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.cache.lru.HeapLRUCapacityController;
import com.gemstone.gemfire.internal.cache.lru.LRUCapacityController;
import com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController;

/**
 * <p>Attributes that describe how a <code>Region</code>'s size is managed through an eviction controller. Eviction
 * controllers are defined by an {@link com.gemstone.gemfire.cache.EvictionAlgorithm} and a {@link
 * com.gemstone.gemfire.cache.EvictionAction}. Once a <code>Region</code> is created with an eviction controller, it can
 * not be removed, however it can be changed through an {@link com.gemstone.gemfire.cache.EvictionAttributesMutator}.
 *
 * @author Mitch Thomas
 * @see com.gemstone.gemfire.cache.AttributesFactory#setEvictionAttributes(EvictionAttributes)
 * @see com.gemstone.gemfire.cache.AttributesMutator
 * @since 5.0
 */
public abstract class EvictionAttributes implements DataSerializable {

  /**
   * <p/>
   * Creates the {@link EvictionAttributes} for an eviction controller that will remove the least recently used (LRU)
   * entry from a region once the region reaches a certain capacity. The entry is either locally destroyed or overflows
   * to disk when evicted by the eviction controller.
   * <p/>
   * <p/>
   * This is not supported when replication is enabled.
   * <p/>
   * <p/>
   * For a region with {@link DataPolicy#PARTITION}, the EvictionAttribute <code>maximum</code>, indicates number of
   * entries allowed in the region, collectively for its primary buckets and redundant copies for this VM. Once there
   * are <code>maximum</code> entries in the region's primary buckets and redundant copies for this VM, the least
   * recently used entry will be evicted from the bucket in which the subsequent put takes place.
   * <p/>
   * <p/>
   * If you are using a <code>cache.xml</code> file to create a Cache Region declaratively, you can include the
   * following to create an LRU entry eviction controller
   * <p/>
   * <pre>
   *         &lt;region-attributes&gt;
   *            &lt;eviction-attributes&gt;
   *               &lt;lru-entry-count maximum=&quot;1000&quot; action=&quot;overflow-to-disk&quot;/&gt;
   *            &lt;/eviction-attributes&gt;
   *         &lt;/region-attributes&gt;
   * </pre>
   *
   * @return a new EvictionAttributes instance
   */
  public static EvictionAttributes createLRUEntryAttributes() {
    return new EvictionAttributesImpl().setAlgorithm(
      EvictionAlgorithm.LRU_ENTRY).setAction(
      EvictionAction.DEFAULT_EVICTION_ACTION).internalSetMaximum(
      LRUCapacityController.DEFAULT_MAXIMUM_ENTRIES);
  }

  /**
   * Create attributes to remove the least recently used entries when the maximum number of entries exists in the
   * Region
   *
   * @param maximumEntries the number of entries to keep in the Region
   * @return a new EvictionAttributes instance
   * @see #createLRUEntryAttributes()
   */
  public static EvictionAttributes createLRUEntryAttributes(int maximumEntries) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setAction(
      EvictionAction.DEFAULT_EVICTION_ACTION).internalSetMaximum(maximumEntries);
  }

  /**
   * Create attributes to perform the given action when the maximum number of entries exists in the Region
   *
   * @return a new EvictionAttributes
   * @see #createLRUEntryAttributes()
   */
  public static EvictionAttributes createLRUEntryAttributes(int maximumEntries, EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_ENTRY).setAction(evictionAction)
      .internalSetMaximum(maximumEntries);
  }

  /**
   * <p/>
   * Create EvictionAttributes for evicting the least recently used {@link Region.Entry} when heap usage exceeds the
   * {@link ResourceManager} eviction heap threshold. If the eviction heap threshold is exceeded the least recently used
   * {@link Region.Entry}s are evicted.
   * <p/>
   * <p/>
   * With other LRU-based eviction controllers, only cache actions (such as {@link Region#put(Object, Object) puts} and
   * {@link Region#get(Object) gets}) cause the LRU entry to be evicted. However, because the JVM's heap may be effected
   * by more than just the GemFire cache operations, a daemon thread will perform the eviction in the event threads are
   * not using the Region.
   * <p/>
   * <p/>
   * When using Heap LRU, the VM must be launched with the <code>-Xmx</code> and <code>-Xms</code> switches set to the
   * same values. Many virtual machine implementations have additional VM switches to control the behavior of the
   * garbage collector. We suggest that you investigate tuning the garbage collector when using this type of eviction
   * controller.  A collector that frequently collects is needed to keep our heap usage up to date. In particular, on
   * the Sun <A href="http://java.sun.com/docs/hotspot/gc/index.html">HotSpot</a> VM, the
   * <code>-XX:+UseConcMarkSweepGC</code> flag needs to be set, and <code>-XX:CMSInitiatingOccupancyFraction=N</code>
   * should be set with N being a percentage that is less than the {@link ResourceManager} eviction heap threshold.
   * <p/>
   * The JRockit VM has similar flags, <code>-Xgc:gencon</code> and <code>-XXgcTrigger:N</code>, which are required if
   * using this LRU algorithm. Please Note: the JRockit gcTrigger flag is based on heap free, not heap in use like the
   * GemFire parameter. This means you need to set gcTrigger to 100-N. for example, if your eviction threshold is 30
   * percent, you will need to set gcTrigger to 70 percent.
   * <p/>
   * On the IBM VM, the flag to get a similar collector is <code>-Xgcpolicy:gencon</code>, but there is no corollary to
   * the gcTrigger/CMSInitiatingOccupancyFraction flags, so when using this feature with an IBM VM, the heap usage
   * statistics might lag the true memory usage of the VM, and thresholds may need to be set sufficiently high that the
   * VM will initiate GC before the thresholds are crossed.
   * <p/>
   * If you are using a <code>cache.xml</code> file to create a Cache Region declaratively, you can include the
   * following to create an LRU heap eviction controller:
   * <p/>
   * <pre>
   *         &lt;region-attributes&gt;
   *            &lt;eviction-attributes&gt;
   *               &lt;lru-heap-percentage action=&quot;overflow-to-disk&quot;
   *            &lt;/eviction-attributes&gt;
   *         &lt;/region-attributes&gt;
   * </pre>
   * <p/>
   * * <p> This is equivalent to calling <code>  createLRUHeapAttributes({@link ObjectSizer#DEFAULT}) </code>
   *
   * @return a new EvictionAttributes instance with {@link EvictionAlgorithm#LRU_HEAP} and the default heap percentage,
   *         eviction interval and eviction action.
   */
  public static EvictionAttributes createLRUHeapAttributes() {
    return new EvictionAttributesImpl()
      .setAlgorithm(EvictionAlgorithm.LRU_HEAP)
      .setAction(EvictionAction.DEFAULT_EVICTION_ACTION)
        // TODO HEAPLRUAPI remove perRegion Heap maximum, use cache
      .internalSetMaximum(HeapLRUCapacityController.DEFAULT_HEAP_PERCENTAGE)
      .setObjectSizer(ObjectSizer.DEFAULT);
  }

  /**
   * Creates EvictionAttributes for evicting the least recently used {@link Region.Entry} when heap usage exceeds the
   * {@link ResourceManager} critical heap threshold.
   *
   * @param sizer the sizer implementation used to determine how many entries to remove
   * @return a new instance of EvictionAttributes with {@link EvictionAlgorithm#LRU_HEAP} and the provided object sizer
   *         and eviction action.
   * @see #createLRUHeapAttributes()
   */
  public static EvictionAttributes createLRUHeapAttributes(final ObjectSizer sizer) {
    return new EvictionAttributesImpl()
      .setAlgorithm(EvictionAlgorithm.LRU_HEAP)
      .setAction(EvictionAction.DEFAULT_EVICTION_ACTION)
      .internalSetMaximum(HeapLRUCapacityController.DEFAULT_HEAP_PERCENTAGE)
      .setObjectSizer(sizer);
  }

  /**
   * Creates EvictionAttributes for evicting the least recently used {@link Region.Entry} when heap usage exceeds the
   * {@link ResourceManager} critical heap threshold.
   *
   * @param sizer the sizer implementation used to determine how many entries to remove
   * @param evictionAction the way in which entries should be evicted
   * @return a new instance of EvictionAttributes with {@link EvictionAlgorithm#LRU_HEAP} and the provided object sizer
   *         and eviction action.
   * @see #createLRUHeapAttributes()
   */
  public static EvictionAttributes createLRUHeapAttributes(final ObjectSizer sizer, final EvictionAction evictionAction) {
    return new EvictionAttributesImpl()
      .setAlgorithm(EvictionAlgorithm.LRU_HEAP)
      .setAction(evictionAction)
      .internalSetMaximum(HeapLRUCapacityController.DEFAULT_HEAP_PERCENTAGE)
      .setObjectSizer(sizer);
  }

  /**
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used (LRU) entry from a
   * region once the region reaches a certain byte capacity. Capacity is determined by monitoring the size of entries
   * added and evicted. Capacity is specified in terms of megabytes. GemFire uses an efficient algorithm to determine
   * the amount of space a region entry occupies in the VM. However, this algorithm may not yield optimal results for
   * all kinds of data. The user may provide his or her own algorithm for determining the size of objects by
   * implementing an {@link ObjectSizer}.
   * <p/>
   * <p/>
   * For a region with {@link DataPolicy#PARTITION}, the EvictionAttribute <code>maximum</code>, is always equal to
   * {@link  PartitionAttributesFactory#setLocalMaxMemory(int)  " local max memory "} specified for the {@link
   * PartitionAttributes}. It signifies the amount of memory allowed in the region, collectively for its primary buckets
   * and redundant copies for this VM. It can be different for the same region in different VMs.
   * <p/>
   * If you are using a <code>cache.xml</code> file to create a Cache Region declaratively, you can include the
   * following to create an LRU memory eviction controller:
   * <p/>
   * <pre>
   *          &lt;region-attributes&gt;
   *            &lt;eviction-attributes&gt;
   *               &lt;lru-memory-size maximum=&quot;1000&quot; action=&quot;overflow-to-disk&quot;&gt;
   *                  &lt;class-name&gt;com.foo.MySizer&lt;/class-name&gt;
   *                  &lt;parameter name=&quot;name&quot;&gt;
   *                     &lt;string&gt;Super Sizer&lt;/string&gt;
   *                  &lt;/parameter&gt;
   *               &lt;/lru-memory-size&gt;
   *            &lt;/eviction-attributes&gt;
   *         &lt;/region-attributes&gt;
   * </pre>
   *
   * @return a new EvictionAttributes
   */
  public static EvictionAttributes createLRUMemoryAttributes() {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY).setAction(
      EvictionAction.DEFAULT_EVICTION_ACTION).internalSetMaximum(MemLRUCapacityController.DEFAULT_MAXIMUM_MEGABYTES)
        .setObjectSizer(ObjectSizer.DEFAULT);
  }

  /**
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used (LRU) entry from a
   * region once the region reaches the given maximum capacity.
   * <p/>
   * <p/>
   * For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the EvictionAttribute
   * <code>maximum</code>, is always set to {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory
   * "} specified for the {@link PartitionAttributes}.
   * <p/>
   * This is equivalent to calling <code>  createLRUMemoryAttributes(maximumMegabytes, {@link ObjectSizer#DEFAULT})
   * </code>
   *
   * @param maximumMegabytes the maximum allowed bytes in the Region
   * @return a new EvictionAttributes
   * @see #createLRUMemoryAttributes()
   */
  public static EvictionAttributes createLRUMemoryAttributes(int maximumMegabytes) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY).setAction(
      EvictionAction.DEFAULT_EVICTION_ACTION).internalSetMaximum(maximumMegabytes).setObjectSizer(null);
  }

  /**
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used (LRU) entry from a
   * region once the region reaches the given maximum capacity.
   * <p/>
   * <p>For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the EvictionAttribute
   * <code>maximum</code>, is always set to {@link  PartitionAttributesFactory#setLocalMaxMemory(int)  " local max
   * memory "} specified for the {@link PartitionAttributes}.
   *
   * @param maximumMegabytes the maximum allowed bytes in the Region
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @return a new EvictionAttributes
   * @see #createLRUMemoryAttributes()
   */
  public static EvictionAttributes createLRUMemoryAttributes(int maximumMegabytes, ObjectSizer sizer) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY).setAction(
      EvictionAction.DEFAULT_EVICTION_ACTION).internalSetMaximum(maximumMegabytes).setObjectSizer(sizer);
  }

  /**
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used (LRU) entry from a
   * region once the region reaches the given maximum capacity.
   * <p/>
   * <p>For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the EvictionAttribute
   * <code>maximum</code>, is always set to {@link  PartitionAttributesFactory#setLocalMaxMemory(int)  " local max
   * memory "} specified for the {@link PartitionAttributes}.
   *
   * @param maximumMegabytes the maximum allowed bytes in the Region
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @param evictionAction the action to take when the maximum has been reached.
   * @return a new EvictionAttributes instance
   * @see #createLRUMemoryAttributes()
   */
  public static EvictionAttributes createLRUMemoryAttributes(int maximumMegabytes, ObjectSizer sizer, EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY).setAction(evictionAction)
      .internalSetMaximum(maximumMegabytes).setObjectSizer(sizer);
  }

  /**
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used (LRU) entry from a
   * region once the region reaches the given maximum capacity.
   * <p/>
   * <p>For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the EvictionAttribute
   * <code>maximum</code>, is always set to {@link  PartitionAttributesFactory#setLocalMaxMemory(int)  " local max
   * memory "} specified for the {@link PartitionAttributes}.
   *
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @return a new EvictionAttributes instance
   * @see #createLRUMemoryAttributes()
   * @since 6.0
   */
  public static EvictionAttributes createLRUMemoryAttributes(ObjectSizer sizer) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY).setAction(
      EvictionAction.DEFAULT_EVICTION_ACTION).setObjectSizer(sizer).internalSetMaximum(
        MemLRUCapacityController.DEFAULT_MAXIMUM_MEGABYTES);
  }

  /**
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used (LRU) entry from a
   * region once the region reaches the given maximum capacity.
   * <p/>
   * <p>For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the EvictionAttribute
   * <code>maximum</code>, is always set to {@link  PartitionAttributesFactory#setLocalMaxMemory(int)  " local max
   * memory "} specified for the {@link PartitionAttributes}.
   *
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @param evictionAction the action to take when the maximum has been reached.
   * @return a new EvictionAttributes instance
   * @see #createLRUMemoryAttributes()
   * @since 6.0
   */
  public static EvictionAttributes createLRUMemoryAttributes(ObjectSizer sizer, EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY).setAction(evictionAction)
      .setObjectSizer(sizer).internalSetMaximum(MemLRUCapacityController.DEFAULT_MAXIMUM_MEGABYTES);
  }

  /**
   * An {@link ObjectSizer} is used by the {@link EvictionAlgorithm#LRU_MEMORY} algorithm to measure the size of each
   * Entry as it is entered into a Region. A default implementation is provided, see {@link
   * #createLRUMemoryAttributes()} for more.
   *
   * @return the sizer used by {@link EvictionAlgorithm#LRU_MEMORY}, for all algorithms null is returned.
   */
  public abstract ObjectSizer getObjectSizer();

  /**
   * The algorithm is used to identify entries that will be evicited.
   *
   * @return a non-null EvictionAlgorithm instance reflecting the configured value or NONE when no eviction controller
   *         has been configured.
   */
  public abstract EvictionAlgorithm getAlgorithm();

  /**
   * The unit of this value is determined by the definition of the {@link EvictionAlgorithm} set by one of the creation
   * methods e.g. {@link EvictionAttributes#createLRUEntryAttributes()}
   *
   * @return maximum value used by the {@link EvictionAlgorithm} which determines when the {@link EvictionAction} is
   *         performed.
   */
  public abstract int getMaximum();

  /** @return action that the {@link EvictionAlgorithm} takes when the maximum value is reached. */
  public abstract EvictionAction getAction();

  @Override
  public final boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof EvictionAttributes)) {
      return false;
    }
    final EvictionAttributes other = (EvictionAttributes) obj;
    if (!this.getAlgorithm().equals(other.getAlgorithm()) ||
        !this.getAction().equals(other.getAction())) {
      return false;
    }
    // LRUHeap doesn't support maximum
    if (!this.getAlgorithm().isLRUHeap() &&
        this.getMaximum() != other.getMaximum()) {
      return false;
    }
    return true;
  }

  @Override
  public final int hashCode() {
    return this.getAlgorithm().hashCode() ^ this.getMaximum();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(128);
    buffer.append(" algorithm=").append(this.getAlgorithm());
    if (!this.getAlgorithm().isNone()) {
      buffer.append("; action=").append(this.getAction());
      if (!getAlgorithm().isLRUHeap()) {
        buffer.append("; maximum=").append(this.getMaximum());
      }
      if (this.getObjectSizer() != null) {
        buffer.append("; sizer=").append(this.getObjectSizer());
      }
    }
    return buffer.toString();
  }

  /**
   * @return an EvictionAttributes for the  LIFOCapacityController
   * @since 5.7
   */
  public static EvictionAttributes createLIFOEntryAttributes(int maximumEntries, EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LIFO_ENTRY).setAction(evictionAction)
      .internalSetMaximum(maximumEntries);
  }

  /**
   * @return an EvictionAttributes for the MemLIFOCapacityController
   * @since 5.7
   */
  public static EvictionAttributes createLIFOMemoryAttributes(int maximumMegabytes, EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LIFO_MEMORY).setAction(evictionAction)
      .internalSetMaximum(maximumMegabytes).setObjectSizer(null);
  }

}
