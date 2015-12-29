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
package com.pivotal.gemfirexd.app.tpce.client;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class TxnGenerator implements Runnable {
	private long txnTotal = 0;
	private long runDuration = 0;
	private static long txnCount = 0;
	final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	
	public TxnGenerator() {};
	public TxnGenerator (long txnTotal, long runDuration) {
		this.txnTotal = txnTotal;
		this.runDuration = runDuration;
	}	

	public void generateTxn() {
		if (runDuration != 0) {
			final ScheduledFuture<?> schedulerHandle = scheduler.scheduleAtFixedRate(this, 0, 0, TimeUnit.SECONDS);		
			scheduler.schedule(new Runnable() {
						public void run() {
							schedulerHandle.cancel(true);
							scheduler.shutdownNow();
						}
					}, this.runDuration, TimeUnit.SECONDS);
		} else {
			while (txnCount < txnTotal) {
				scheduler.submit(this);
			}
		}
	}

	public void run() {
		this.txnCount++;
	}
}
