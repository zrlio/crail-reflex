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

package com.ibm.crail.datanode.reflex;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.conf.CrailConstants;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.datanode.reflex.client.ReFlexStorageEndpoint;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.storage.StorageTier;
import com.ibm.crail.namenode.protocol.DataNodeStatistics;
import com.ibm.disni.reflex.ReFlexEndpointGroup;
import org.apache.commons.cli.*;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Arrays;

public class ReFlexStorageTier extends StorageTier {

	private static final Logger LOG = CrailUtils.getLogger();
	private InetSocketAddress datanodeAddr;
	private ReFlexEndpointGroup clientGroup;

	public InetSocketAddress getAddress() {
		if (datanodeAddr == null) {
			datanodeAddr = new InetSocketAddress(ReFlexStorageConstants.IP_ADDR, ReFlexStorageConstants.PORT);
		}
		return datanodeAddr;
	}

	public void printConf(Logger logger) {
		ReFlexStorageConstants.printConf(logger);
	}

	public void init(CrailConfiguration crailConfiguration, String[] args) throws IOException {
		ReFlexStorageConstants.updateConstants(crailConfiguration);

		if (args != null) {
			Options options = new Options();
			Option bindIp = Option.builder("a").desc("ip address to bind to").hasArg().build();
			Option port = Option.builder("p").desc("port to bind to").hasArg().type(Number.class).build();
			//Option pcieAddress = Option.builder("s").desc("PCIe address of NVMe device").hasArg().build();
			options.addOption(bindIp);
			options.addOption(port);
			//options.addOption(pcieAddress);
			CommandLineParser parser = new DefaultParser();
			HelpFormatter formatter = new HelpFormatter();
			CommandLine line = null;
			try {
				line = parser.parse(options, Arrays.copyOfRange(args, 2, args.length));
				if (line.hasOption(port.getOpt())) {
					ReFlexStorageConstants.PORT = ((Number) line.getParsedOptionValue(port.getOpt())).intValue();
				}
			} catch (ParseException e) {
				System.err.println(e.getMessage());
				formatter.printHelp("ReFlex storage tier", options);
				System.exit(-1);
			}
			if (line.hasOption(bindIp.getOpt())) {
				ReFlexStorageConstants.IP_ADDR = InetAddress.getByName(line.getOptionValue(bindIp.getOpt()));
			}
			//if (line.hasOption(pcieAddress.getOpt())) {
			//	NvmfStorageConstants.PCIE_ADDR = line.getOptionValue(pcieAddress.getOpt());
			//}
		}

		ReFlexStorageConstants.verify();
	}

	public StorageEndpoint createEndpoint(InetSocketAddress inetSocketAddress) throws IOException {
		if (clientGroup == null) {
			synchronized (this) {
				if (clientGroup == null) {
					clientGroup = new ReFlexEndpointGroup();
				}
			}
		}
		return new ReFlexStorageEndpoint(clientGroup, inetSocketAddress);
	}

	public void run() throws Exception {
		LOG.info("initalizing ReFlex datanode");

		ReFlexEndpointGroup group = new ReFlexEndpointGroup();
		
		long namespaceSize = 0x5d27216000L / 512 ; // for Intel device
		long addr = 0;
		this.setBlock(addr, (int)namespaceSize, 0); // single allocation for whole namespace

		while (true) {
			DataNodeStatistics statistics = this.getDataNode();
			LOG.info("datanode statistics, freeBlocks " + statistics.getFreeBlockCount());
			Thread.sleep(2000);
		}

	}

	public void close() throws Exception {

	}
}
