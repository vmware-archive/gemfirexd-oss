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
package hydra.training.dunit;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;
import hydra.RmiRegistryHelper;
import hydra.training.RemoteBlockingQueue;
import hydra.training.RemoteBlockingQueueImpl;
import java.rmi.Naming;
//import java.rmi.RemoteException;

/**
 * A DUnit distributed unit test that exercises the basic
 * functionality of the {@link RemoteBlockingQueue}.  Note that most
 * methods throw <code>Exception</code> because if anything goes
 * wrong, the test should fail.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class RemoteBlockingQueueTest extends DistributedTestCase {

  /** The maximum number of elements allowed in the distributed queue
   * used by this test. */
  private static final int QUEUE_CAPACITY = 10;

  /** The RMI URL of the remote queue */
  private String queueURL;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>RemoteBlockingQueueTest</code> with the given
   * name.
   */
  public RemoteBlockingQueueTest(String name) {
    super(name);
  }

  /////////////////////  Helper Methods  //////////////////////

  /**
   * Initializes this test by binding an instance of
   * <code>RemoteBlockingQueueImpl</code> into the RMI registry hosted
   * in Hydra's master controller VM.
   */
  public void setUp() throws Exception {
    String queueName = this.getUniqueName();
    Host host = Host.getHost(0);
    this.queueURL = RmiRegistryHelper.getMasterRegistryURL() + queueName;

    RemoteBlockingQueue queue =
      new RemoteBlockingQueueImpl(QUEUE_CAPACITY);
    DistributedTestCase.getLogWriter().info("Binding queue named \"" + this.queueURL
                             + "\"");
    Naming.bind(this.queueURL, queue);
  }

  /**
   * Cleans up after this test by unbinding the instance of
   * <code>RemoteBlockingQueue</code> that was initialized in {@link
   * #setUp}.
   */
  public void tearDown2() throws Exception {
    Naming.unbind(this.queueURL);
  }

  /**
   * Returns the <code>RemoteBlockingQueue</code> used by this test.
   */
  protected RemoteBlockingQueue getQueue() throws Exception {
    return (RemoteBlockingQueue) Naming.lookup(this.queueURL);
  }

  ///////////////////////  Test Methods  ///////////////////////

  /**
   * Tests that an element <code>put</code> by one VM can be seen by
   * another VM.
   */
  public void testDistributedPut() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    final Object element = new Integer(123);

    vm0.invoke(new SerializableRunnable("Put element") {
        public void run() {
          try {
            RemoteBlockingQueue queue = getQueue();
            boolean offered = queue.offer(element);
            assertTrue(offered);

          } catch (Exception ex) {
            fail("Unexpected exception", ex);
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Get element") {
        public void run() {
          try {
            RemoteBlockingQueue queue = getQueue();
            Object removed = queue.poll();
            assertEquals(element, removed);

          } catch (Exception ex) {
            fail("Unexpected exception", ex);
          }
        }
      });
  }

  /**
   * Tests that elements added multiple producers are consumed in the
   * same order.
   */
  public void testProducersConsumer() throws Exception {
    Host host = Host.getHost(0);
    final int maxElements = 100;
    final int vmCount = host.getVMCount();

    VM vm0 = host.getVM(0);
    AsyncInvocation consumer =
      vm0.invokeAsync(new SerializableRunnable("Consumer") {
          public void run() {
            int[] nextCounts = new int[vmCount - 1];
            try {
              RemoteBlockingQueue queue = getQueue();
              for (int i = 0; i < maxElements * (vmCount - 1); i++) {
                int value = ((Integer) queue.take()).intValue();
                getLogWriter().info("Consumed " + value);
                int whichVM = value / maxElements;
                int count = value % maxElements;
                assertEquals(nextCounts[whichVM], count);
                nextCounts[whichVM] = count + 1;
              }

            } catch (Exception ex) {
              fail("While consuming", ex);
            }
          }
        });
    

    AsyncInvocation[] producers = new AsyncInvocation[vmCount - 1];
    for (int i = 1; i <= vmCount - 1; i++) {
      final int whichVM = i;
      VM vm = host.getVM(whichVM);
      producers[whichVM - 1] =
        vm.invokeAsync(new SerializableRunnable("Produce") {
            public void run() {
              try {
                RemoteBlockingQueue queue = getQueue();
                for (int j = 0; j < maxElements; j++) {
                  int value = ((whichVM - 1) * maxElements) + j;
                  getLogWriter().info("Produced " + value);
                  queue.put(new Integer(value));
                }
              } catch (Exception ex) {
                fail("While producing", ex);
              }
            }
          });
    }

    // Wait for threads to finish
    for (int i = 0; i < producers.length; i++) {
      producers[i].join();
      if (producers[i].exceptionOccurred()) {
        fail("Producer " + i + " failed",
             producers[i].getException());
      }
    }
    consumer.join();
    if (consumer.exceptionOccurred()) {
      fail("Consumer failed", consumer.getException());
    }

    // Make sure queue is empty
    assertNull(getQueue().peek());
  }

}
