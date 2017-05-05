package com.ibm.crail.storage.reflex.client;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.ibm.crail.CrailBuffer;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.memory.BufferCache;
import com.ibm.crail.memory.OffHeapBuffer;

public class ReFlexBufferCache extends BufferCache {

	public ReFlexBufferCache() throws IOException {
		super();
	}

	@Override
	public CrailBuffer allocateBuffer() throws IOException {
		ByteBuffer buffer = ByteBuffer.allocateDirect(CrailConstants.BUFFER_SIZE);
		return OffHeapBuffer.wrap(buffer);
	}

}
