package com.ibm.crail.storage.reflex.client;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.reflex.ReFlexStorageConstants;
import com.ibm.crail.utils.CrailUtils;

import org.slf4j.Logger;

import com.ibm.reflex.client.ReflexClientGroup;
import com.ibm.reflex.client.ReflexEndpoint;

import java.io.IOException;

public class ReflexStorageClient implements StorageClient {
    private ReflexClientGroup clientGroup;

    public void printConf(Logger logger) {
        ReFlexStorageConstants.printConf(logger);
    }

    public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
        ReFlexStorageConstants.updateConstants(crailConfiguration);
        ReFlexStorageConstants.verify();
        clientGroup = new ReflexClientGroup(ReFlexStorageConstants.QUEUE_SIZE, ReFlexStorageConstants.BLOCK_SIZE, ReFlexStorageConstants.NO_DELAY);
    }

    public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
        try {
        	ReflexEndpoint endpoint = clientGroup.createEndpoint();
        	endpoint.connect(CrailUtils.datanodeInfo2SocketAddr(info));
            return new ReFlexStorageEndpoint(endpoint);
        } catch(Exception e){
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
