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

import java.util.concurrent.TimeUnit;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * A blocking queue that is shared among multiple Java programs
 * potentially running on multiple machines.  It is inspiried by the
 * JSR-166 {@link java.util.concurrent.BlockingQueue}
 * and is implemented using standard Java {@link java.rmi RMI}.
 *
 * @author David Whitlock
 * @since 4.0
 */
public interface RemoteBlockingQueue extends Remote {

    /**
     * Inserts the specified element into this queue, if
     * possible. When using queues that may impose insertion
     * restrictions (for example capacity bounds), method
     * <tt>offer</tt> is generally preferable to method {@link
     * java.util.Collection#add}, which can fail to insert an element
     * only by throwing an exception.
     *
     * @param o the element to add.
     * @return <tt>true</tt> if it was possible to add the element to
     *     this queue, else <tt>false</tt>
     * @throws NullPointerException 
     *         if the specified element is <tt>null</tt>.
     */ 
    boolean offer(Object o) throws RemoteException;
    
    /**
     * Inserts the specified element into this queue, waiting if necessary
     * up to the specified wait time for space to become available.
     *
     * @param o the element to add
     * @param timeout how long to wait before giving up, in units of
     * <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the
     * <tt>timeout</tt> parameter
     * @return <tt>true</tt> if successful, or <tt>false</tt> if
     * the specified waiting time elapses before space is available.
     * @throws InterruptedException if interrupted while waiting.
     * @throws NullPointerException if the specified element is <tt>null</tt>.
     */
    boolean offer(Object o, long timeout, TimeUnit unit)
        throws InterruptedException, RemoteException;    

    /**
     * Retrieves and removes the head of this queue, waiting
     * if necessary up to the specified wait time if no elements are
     * present on this queue.
     * @param timeout how long to wait before giving up, in units of
     * <tt>unit</tt>
     * @param unit a <tt>TimeUnit</tt> determining how to interpret the
     * <tt>timeout</tt> parameter
     * @return the head of this queue, or <tt>null</tt> if the
     * specified waiting time elapses before an element is present.
     * @throws InterruptedException if interrupted while waiting.
     */
    Object poll(long timeout, TimeUnit unit)
        throws InterruptedException, RemoteException;
    
    /**
     * Retrieves and removes the head of this queue. This method
     * differs from the <tt>poll</tt> method in that it throws an
     * exception if this queue is empty.
     *
     * @return the head of this queue.
     * @throws NoSuchElementException if this queue is empty.
     */
    Object poll() throws RemoteException;

    /**
     * Retrieves and removes the head of this queue, waiting
     * if no elements are present on this queue.
     * @return the head of this queue
     * @throws InterruptedException if interrupted while waiting.
     */
    Object take() throws InterruptedException, RemoteException;

    /**
     * Adds the specified element to this queue, waiting if necessary for
     * space to become available.
     * @param o the element to add
     * @throws InterruptedException if interrupted while waiting.
     * @throws NullPointerException if the specified element is <tt>null</tt>.
     */
    void put(Object o) throws InterruptedException, RemoteException;

    /**
     * Retrieves, but does not remove, the head of this queue,
     * returning <tt>null</tt> if this queue is empty.
     *
     * @return the head of this queue, or <tt>null</tt> if this queue
     * is empty.
     */
    Object peek() throws RemoteException;

}
