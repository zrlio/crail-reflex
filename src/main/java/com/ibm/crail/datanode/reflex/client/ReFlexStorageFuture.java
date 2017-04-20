/*
 * Crail: A Multi-tiered Distributed Direct Access File System
 *
 * Author:
 * Jonas Pfefferle <jpf@zurich.ibm.com>
 *
 * Copyright (C) 2016, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.crail.datanode.reflex.client;

import com.ibm.crail.datanode.reflex.ReFlexStorageConstants;
import com.ibm.crail.storage.StorageFuture;
import com.ibm.crail.storage.StorageResult;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ReFlexStorageFuture implements StorageFuture, StorageResult {

	private final ReFlexStorageEndpoint endpoint;
	private final int len;
	private Exception exception;
	private volatile boolean done;

	public ReFlexStorageFuture(ReFlexStorageEndpoint endpoint, int len) {
		this.endpoint = endpoint;
		this.len = len;
	}

	public int getLen() {
		return len;
	}

	public boolean cancel(boolean b) {
		return false;
	}

	public boolean isCancelled() {
		return false;
	}

	//void signal(NvmeStatusCodeType statusCodeType, int statusCode) {
	void signal(int statusCode) {
		if (statusCode != 0) { //FIXME: hard-coded success --> statusCode == 0
			exception = new ExecutionException("Error: status code is " + statusCode) {};
		}
		done = true;
	}

	public boolean isDone() {
		if (!done) {
			try {
				endpoint.poll();
			} catch (IOException e) {
				exception = e;
			}
		}
		return done;
	}

	public StorageResult get() throws InterruptedException, ExecutionException {
		try {
			return get(ReFlexStorageConstants.TIME_OUT, ReFlexStorageConstants.TIME_UNIT);
		} catch (TimeoutException e) {
			throw new ExecutionException(e);
		}
	}

	public StorageResult get(long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {

		if (exception != null) {
			throw new ExecutionException(exception);
		}
		if (!done) {
			long start = System.nanoTime();
			long end = start + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);
			boolean waitTimeOut;
			do {
				try {
					endpoint.poll();
				} catch (IOException e) {
					throw new ExecutionException(e);
				}
				// we don't want to trigger timeout on first iteration
				waitTimeOut = System.nanoTime() > end;
			} while (!done && !waitTimeOut);
			if (!done && waitTimeOut) {
				throw new TimeoutException("get wait time out!");
			}
			if (exception != null) {
				throw new ExecutionException(exception);
			}
		}
		return this;
	}

	@Override
	public boolean isSynchronous() {
		return false;
	}
}
