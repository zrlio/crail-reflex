package com.ibm.crail.storage.reflex.client;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageClient;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.reflex.ReFlexStorageConstants;
import com.ibm.crail.utils.CrailUtils;
import org.slf4j.Logger;
import stanford.mast.reflex.ReFlexEndpointGroup;

import java.io.IOException;

public class ReflexStorageClient implements StorageClient {
    private static final Logger LOG = CrailUtils.getLogger();
    private ReFlexEndpointGroup clientGroup;

    public void printConf(Logger logger) {
        ReFlexStorageConstants.printConf(logger);
    }

    public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
        ReFlexStorageConstants.updateConstants(crailConfiguration);

        ReFlexStorageConstants.verify();

        clientGroup = new ReFlexEndpointGroup();
    }

    public StorageEndpoint createEndpoint(DataNodeInfo info) throws IOException {
        try {
            return new ReFlexStorageEndpoint(clientGroup, CrailUtils.datanodeInfo2SocketAddr(info));
        } catch(Exception e){
            throw new IOException(e);
        }
    }

    @Override
    public void close() throws Exception {
    }
}
