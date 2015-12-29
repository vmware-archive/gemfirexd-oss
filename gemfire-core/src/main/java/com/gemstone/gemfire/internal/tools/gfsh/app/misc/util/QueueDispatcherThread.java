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
package com.gemstone.gemfire.internal.tools.gfsh.app.misc.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Collections;


/**
 * <p>Title:</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2004</p>
 * <p>Company: GemStone Systems, Inc.</p>
 * @author Dae Song Park
 * @version 1.0
 */
public class QueueDispatcherThread extends Thread
{
    private List list = Collections.synchronizedList(new LinkedList());
    private QueueDispatcherListener queueDispatcherListener;
    private boolean shouldRun = true;

    public QueueDispatcherThread()
    {
        setDaemon(true);
    }

    public synchronized void enqueue(Object obj)
    {
        list.add(obj);
        this.notify();
    }

    public synchronized Object dequeue() throws InterruptedException
    {
        while (list.size() == 0) {
            this.wait(1000);
        }
        return list.remove(0);
    }

    public int size()
    {
        return list.size();
    }

    public boolean isEmpty()
    {
        return list.size() == 0;
    }

    public void setQueueDispatcherListener(QueueDispatcherListener listener)
    {
        this.queueDispatcherListener = listener;
    }

    public QueueDispatcherListener getQueueDispatcherListener()
    {
        return queueDispatcherListener;
    }

    public synchronized void run()
    {
        while (shouldRun) {
            try {
            	while (list.size() == 0 && shouldRun) {
                    this.wait(1000);
                }
            	int size = list.size();
            	if (size > 0) {
            		for (int i = 0; i < size; i++) {
            			Object obj = list.remove(0);
		                if (queueDispatcherListener != null) {
		                    queueDispatcherListener.objectDispatched(obj);
		                }
            		}
            	}
            } catch (InterruptedException ex) {
                // ignore for the time being
            }
        }
    }

    public void terminate()
    {
        shouldRun = false;
    }
}
