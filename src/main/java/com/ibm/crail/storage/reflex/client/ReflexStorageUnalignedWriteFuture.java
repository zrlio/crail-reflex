package com.ibm.crail.storage.reflex.client;

import com.ibm.crail.CrailBuffer;
import java.io.IOException;


public class ReflexStorageUnalignedWriteFuture extends ReFlexStorageFuture {

	private CrailBuffer stagingBuffer;

	public ReflexStorageUnalignedWriteFuture(ReFlexStorageEndpoint endpoint, int len, CrailBuffer stagingBuffer) {
		super(endpoint, len);
		this.stagingBuffer = stagingBuffer;
	}

	@Override
	void signal(int statusCode) {
		try {
			endpoint.putBuffer(stagingBuffer);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(-1);
		}
		super.signal(statusCode);
	}
}
