package com.ibm.crail.datanode.reflex;

import java.net.InetSocketAddress;
import org.slf4j.Logger;
import com.ibm.crail.storage.StorageRpcClient;
import com.ibm.crail.storage.StorageServer;
import com.ibm.crail.utils.CrailUtils;

public class ReFlexStorageServer implements StorageServer {
	private static final Logger LOG = CrailUtils.getLogger();
	
	private boolean alive;
	
	public ReFlexStorageServer(){
		this.alive = false;
	}

	@Override
	public InetSocketAddress getAddress() {
		return new InetSocketAddress(ReFlexStorageConstants.IP_ADDR, ReFlexStorageConstants.PORT);
	}

	@Override
	public boolean isAlive() {
		return alive;
	}

	@Override
	public void registerResources(StorageRpcClient namenodeClient) throws Exception {
		LOG.info("initalizing ReFlex datanode");
		long namespaceSize = 0x5d27216000L / 512 ; // for Intel device
		namenodeClient.setBlock(0, (int) namespaceSize, 0);
		this.alive = true;
	}

}
