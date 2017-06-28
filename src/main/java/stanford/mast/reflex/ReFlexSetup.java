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

import com.ibm.disni.util.MemoryAllocation;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// equivalent of Nvme class for Nvmef
// does general setup for ReFlex endpoint group
public class ReFlexSetup {

	private final NativeDispatcher nativeDispatcher;
	//private final MemoryAllocation memoryAllocation;

	public ReFlexSetup(NativeDispatcher nativeDispatcher) throws IllegalArgumentException {
		this.nativeDispatcher = nativeDispatcher;
	}
	
	//FIXME: for IX client, setup DPDK environment here...
	public ReFlexSetup(String hugePath, long[] socketMemoryMB, NativeDispatcher nativeDispatcher) throws IllegalArgumentException {
		this.nativeDispatcher = nativeDispatcher;
		//memoryAllocation = MemoryAllocation.getInstance();
		/*
		ArrayList<String> args = new ArrayList<String>();
		args.add("nvme");
		if (!pcie) {
			args.add("--no-pci");
		}

		if (hugePath == null) {
			//FIXME: this does not seem to work with the current SPDK build
			args.add("--no-huge");
			long totalMemory = 0;
			for (long memory : socketMemoryMB) {
				totalMemory += memory;
			}

			args.add("-m");
			args.add(Long.toString(totalMemory));
		} else {
			args.add("--huge-dir");
			args.add(hugePath);

			args.add("--socket-mem");
			if (socketMemoryMB == null || socketMemoryMB.length == 0) {
				throw new IllegalArgumentException("socketMemoryMB null or zero length");
			}
			StringBuilder sb = new StringBuilder();
			for (long memory : socketMemoryMB) {
				if (sb.length() > 0) {
					sb.append(',');
				}
				sb.append(Long.toString(memory));
			}
			args.add(sb.toString());
		}

		args.add("--proc-type");
		args.add("primary");

		int ret = nativeDispatcher._rte_eal_init(args.toArray(new String[args.size()]));
		if (ret < 0) {
			throw new IllegalArgumentException("rte_eal_init failed with " + ret);
		}
		*/
	}

	/*
	public void logEnableTrace() {
		nativeDispatcher._log_set_trace_flag("all");
	}
	*/


	public ByteBuffer allocateBuffer(int size, int alignment) {
		if (size < 0) {
			throw new IllegalArgumentException("negative size");
		}
		if (alignment < 0) {
			throw new IllegalArgumentException("negative alignment");
		}
		long address = nativeDispatcher._malloc(size, alignment);
		if (address == 0) {
			throw new OutOfMemoryError("No more space when try malloc");
		}

		Class directByteBufferClass;
		try {
			directByteBufferClass = Class.forName("java.nio.DirectByteBuffer");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("No class java.nio.DirectByteBuffer");
		}
		Constructor<Object> constructor = null;
		try {
			constructor = directByteBufferClass.getDeclaredConstructor(Long.TYPE, Integer.TYPE);
		} catch (NoSuchMethodException e) {
			throw new RuntimeException("No constructor (long, int) in java.nio.DirectByteBuffer");
		}
		constructor.setAccessible(true);
		ByteBuffer buffer;
		try {
			buffer = (ByteBuffer)constructor.newInstance(address, size);
		} catch (InstantiationException e) {
			throw new RuntimeException(e);
		} catch (IllegalAccessException e) {
			throw new RuntimeException(e);
		} catch (InvocationTargetException e) {
			throw new RuntimeException(e);
		}

		return buffer;
	}

	public void freeBuffer(ByteBuffer buffer) {
		nativeDispatcher._free(((DirectBuffer)buffer).address());
	}
}
