/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Jonas Pfefferle <jpf@zurich.ibm.com>
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

package stanford.mast.reflex;

import com.ibm.disni.util.MemBuf;
import com.ibm.disni.util.MemoryAllocation;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class IOCompletion {
	public final static int CSIZE = 24;

	private final static int STATUS_CODE_TYPE_OFFSET = 0;
	private final static int STATUS_CODE_OFFSET = 4;
	private final static int ID_OFFSET = 8;
	private final static int COMPLETED_ARRAY_OFFSET = 16;

	private static final int INVALID_STATUS_CODE_TYPE = -1;


	private int statusCodeType;
	private final MemBuf memBuf;
	private final ByteBuffer buffer;
	private final long address;
	private boolean pending;
	private long id = 0;

	// Completed Array (note: this is in NvmeQueuePair for nvmef) 
	//private final ByteBuffer completedArray;
	//private final long completedArrayAddress;
	//private static final int COMPLETED_ARRAY_INDEX_OFFSET = 0;
	//private static final int COMPLETED_ARRAY_START_OFFSET = 8;
	//private static final int COMPLETED_ARRAY_SIZE = 1024;


	public IOCompletion() {
		MemoryAllocation memoryAllocation = MemoryAllocation.getInstance();
		memBuf = memoryAllocation.allocate(CSIZE, MemoryAllocation.MemType.DIRECT,
				IOCompletion.class.getCanonicalName());
		buffer = memBuf.getBuffer();
		buffer.order(ByteOrder.nativeOrder());
		address = ((DirectBuffer)buffer).address();
		pending = false;

		// completed array init
		//this.completedArray = ByteBuffer.allocateDirect(COMPLETED_ARRAY_SIZE * 8 + COMPLETED_ARRAY_START_OFFSET);
		//this.completedArray.order(ByteOrder.nativeOrder());
		//this.completedArrayAddress = ((DirectBuffer)completedArray).address();
	}

	void reset() throws PendingOperationException {
		if (statusCodeType == INVALID_STATUS_CODE_TYPE) {
			throw new PendingOperationException();
		}
		this.statusCodeType = INVALID_STATUS_CODE_TYPE;
		buffer.putInt(STATUS_CODE_TYPE_OFFSET, INVALID_STATUS_CODE_TYPE);
		pending = true;
	}

	public void free() {
		memBuf.free();
	}

	void setCompletedArray(ReFlexEndpoint endpoint) {
		buffer.putLong(COMPLETED_ARRAY_OFFSET, endpoint.getCompletedArrayAddress());
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
		buffer.putLong(ID_OFFSET, id);
	}

	long address() {
		return address;
	}

	public boolean isPending() {
		return pending;
	}

	//public NvmeStatusCodeType getStatusCodeType() {
	public int getStatusCodeType() {
		//assert buffer.getInt(STATUS_CODE_TYPE_OFFSET) == statusCodeType;
		return statusCodeType;
	}

	public int getStatusCode() {
		return buffer.getInt(STATUS_CODE_OFFSET);
	}

	public boolean done() {
		if (statusCodeType == INVALID_STATUS_CODE_TYPE) {
			/* Update */
			statusCodeType = buffer.getInt(STATUS_CODE_TYPE_OFFSET);
		}
		pending = statusCodeType == INVALID_STATUS_CODE_TYPE;
		return !pending;
	}
}
