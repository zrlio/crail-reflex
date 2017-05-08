package com.ibm.crail.storage.reflex;

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
		LOG.info("initalizing ReFlex storage");

		long namespaceSize = 0x1749a956000L; // for Samsung PM1725 device
		//long namespaceSize = 0x5d27216000L // for Intel PM3600 device
		long alignedSize = namespaceSize - (namespaceSize % ReFlexStorageConstants.ALLOCATION_SIZE);

		long addr = 0;
		while (alignedSize > 0) {
			//DataNodeStatistics statistics = namenodeClient.getDataNode();
			//LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());

			LOG.info("new block, length " + ReFlexStorageConstants.ALLOCATION_SIZE);
			LOG.debug("block stag 0, addr " + addr + ", length " + ReFlexStorageConstants.ALLOCATION_SIZE);
			alignedSize -= ReFlexStorageConstants.ALLOCATION_SIZE;
			namenodeClient.setBlock(addr, (int)ReFlexStorageConstants.ALLOCATION_SIZE, 0);
			addr += ReFlexStorageConstants.ALLOCATION_SIZE;
		}
		

		this.alive = true;

	}

}
