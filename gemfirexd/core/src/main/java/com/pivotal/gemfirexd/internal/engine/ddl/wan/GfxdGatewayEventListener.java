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
package com.pivotal.gemfirexd.internal.engine.ddl.wan;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.pivotal.gemfirexd.callbacks.AsyncEventListener;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;

/**
 * 
 * @author Asif, Yogesh
 * 
 */
public final class GfxdGatewayEventListener implements
    com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener<Object, Object> {

  private AsyncEventListener asyncListener;
  
  private final String initParams;

  // This boolean will be acted upon by only a single dispatcher thread
  // there are other synch blocks in Gateway etc which will ensure that
  // in case of gateway stopping & restarting this boolean would be commited
  // without causing visibility issues.
  // Ideally some init method or disptachReadiness method of Listener should be
  // invoked by the dispatcher thread as soon as it gets started & before it
  // acts on the queue
  private boolean initialReplayDone = false;

  public GfxdGatewayEventListener(final AsyncEventListener wbclListener, final String initParams) {
    this.asyncListener = wbclListener;
    this.initParams = initParams;
  }

  public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
    if (!this.initialReplayDone) {
      GemFireXDUtils.waitForNodeInitialization();
      this.initialReplayDone = true;
    }
    return this.asyncListener.processEvents(new EventList(events));
  }

  public void start() {
    this.asyncListener.start();
  }
  
  public void close() {
    this.asyncListener.close();
  }

  public void refreshActualListener(AsyncEventListener newListener) {
    newListener.init(this.initParams);
    this.asyncListener.close();
    this.asyncListener = newListener;
  }
  /**
   * Reinitializes the queue 
   */
  public void reInit() {
    this.asyncListener.init(initParams);
  }

  private static class EventList implements List<Event> {

    private final List<AsyncEvent<Object, Object>> gatewayEvents;

    EventList(List<AsyncEvent<Object, Object>> gatewayEvents) {
      this.gatewayEvents = gatewayEvents;
    }

    public boolean add(Event o) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public void add(int index, Event element) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public boolean addAll(Collection<? extends Event> c) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public boolean addAll(int index, Collection<? extends Event> c) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public void clear() {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public boolean contains(Object o) {
      if (o instanceof WBCLEventImpl) {
        return this.gatewayEvents
            .contains(((WBCLEventImpl)o).getAsyncEvent());
      }
      else {
        return false;
      }
    }

    public boolean containsAll(Collection<?> c) {
      boolean containsAll = true;
      Iterator<?> itr = c.iterator();
      while (itr.hasNext()) {
        containsAll = containsAll && this.contains(itr.next());
        if (!containsAll) {
          break;
        }
      }
      return containsAll;
    }

    public Event get(int index) {
      AsyncEvent<Object, Object> event = this.gatewayEvents.get(index);
      return GemFireXDUtils.convertToEvent(event);
    }

    public int indexOf(Object o) {
      int index = -1;
      if (o instanceof WBCLEventImpl) {
        index = this.gatewayEvents.indexOf(((WBCLEventImpl)o).getAsyncEvent());
      }
      return index;
    }

    public boolean isEmpty() {

      return this.gatewayEvents.isEmpty();
    }

    public Iterator<Event> iterator() {
      final Iterator<AsyncEvent<Object, Object>> internalItr = this.gatewayEvents
          .iterator();

      return new Iterator<Event>() {

        public boolean hasNext() {
          return internalItr.hasNext();
        }

        public Event next() {
          AsyncEvent<Object, Object> event = internalItr.next();
          return GemFireXDUtils.convertToEvent(event);
        }

        public void remove() {
          throw new UnsupportedOperationException(
              "The List passed as callback "
                  + "parameter is unmodifiable so not implementing it");
        }
      };
    }

    public int lastIndexOf(Object o) {
      if (o instanceof WBCLEventImpl) {
        return this.gatewayEvents.lastIndexOf(((WBCLEventImpl)o).getAsyncEvent());
      }
      else {
        return -1;
      }
    }

    public ListIterator<Event> listIterator() {
      return createWrappedListIterator(this.gatewayEvents.listIterator());
    }

    private static ListIterator<Event> createWrappedListIterator(
        final ListIterator<AsyncEvent<Object, Object>> listItr) {
      return new ListIterator<Event>() {

        public void add(Event o) {
          throw new UnsupportedOperationException(
              "The List passed as callback "
                  + "parameter is unmodifiable so not implementing it");
        }

        public boolean hasNext() {
          return listItr.hasNext();
        }

        public boolean hasPrevious() {
          return listItr.hasPrevious();
        }

        public Event next() {
          AsyncEvent<Object, Object> event = listItr.next();
          return GemFireXDUtils.convertToEvent(event);
        }

        public int nextIndex() {
          return listItr.nextIndex();
        }

        public Event previous() {
          AsyncEvent<Object, Object> event = listItr.previous();
          return GemFireXDUtils.convertToEvent(event);
        }

        public int previousIndex() {
          return listItr.previousIndex();
        }

        public void remove() {
          throw new UnsupportedOperationException(
              "The List passed as callback "
                  + "parameter is unmodifiable so not implementing it");
        }

        public void set(Event o) {
          throw new UnsupportedOperationException(
              "The List passed as callback "
                  + "parameter is unmodifiable so not implementing it");
        }

      };
    }

    public ListIterator<Event> listIterator(int index) {
      return createWrappedListIterator(this.gatewayEvents.listIterator(index));
    }

    public boolean remove(Object o) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public Event remove(int index) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public boolean removeAll(Collection<?> c) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public boolean retainAll(Collection<?> c) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public Event set(int index, Event element) {
      throw new UnsupportedOperationException("The List passed as callback "
          + "parameter is unmodifiable so not implementing it");
    }

    public int size() {
      return this.gatewayEvents.size();
    }

    public List<Event> subList(int fromIndex, int toIndex) {
      return new EventList(this.gatewayEvents.subList(fromIndex, toIndex));
    }

    public Object[] toArray() {
      Object[] objs = new Object[this.gatewayEvents.size()];
      Iterator<AsyncEvent<Object, Object>> geItr = this.gatewayEvents
          .iterator();
      int i = 0;
      while (geItr.hasNext()) {
        objs[i++] = GemFireXDUtils.convertToEvent(geItr.next());
      }
      return objs;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a) {
      final int size = size();
      if (a.length < size) {
        a = (T[])java.lang.reflect.Array.newInstance(a.getClass()
            .getComponentType(), size);
      }
      Iterator<AsyncEvent<Object, Object>> evItr = this.gatewayEvents
          .iterator();
      int i = 0;
      while (evItr.hasNext()) {
        a[i++] = (T)GemFireXDUtils.convertToEvent(evItr.next());
      }
      return a;
    }
  }

  /**
   * TEST API.
   * @return
   */
  public AsyncEventListener getAsyncEventListenerForTest() {
    return this.asyncListener;
  }
}
