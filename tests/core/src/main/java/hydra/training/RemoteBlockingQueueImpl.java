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
package hydra.training;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 * An implementation of <code>RemoteBlockingQueue</code> that uses a
 * JSR-166 {@link BlockingQueue} to do the real work.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class RemoteBlockingQueueImpl extends UnicastRemoteObject
  implements RemoteBlockingQueue {

  /** The queue that does the real work */
  private BlockingQueue queue;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>RemoteBlockingQueueImpl</code> with the given
   * maximum number of elements.
   */
  public RemoteBlockingQueueImpl(int capacity) 
    throws RemoteException {

    super();                    // Initialize RMI stuff
    this.queue = new LinkedBlockingQueue(capacity);
  }

  //////////////////////  Instance Methods  /////////////////////

  public boolean offer(Object o) throws RemoteException {
    return this.queue.offer(o);
  }

  public boolean offer(Object o, long timeout, TimeUnit unit)
    throws InterruptedException, RemoteException {

    return this.queue.offer(o, timeout, unit);
  }

  public Object poll(long timeout, TimeUnit unit)
    throws InterruptedException, RemoteException {

    return this.queue.poll(timeout, unit);
  }
  
  public Object poll() throws RemoteException {
    return this.queue.poll();
  }

  public Object take() throws InterruptedException, RemoteException {
    return this.queue.take();
  }

  public void put(Object o) throws InterruptedException, RemoteException {
    this.queue.put(o);
  }

  public Object peek() throws RemoteException {
    return this.queue.peek();
  }

}
