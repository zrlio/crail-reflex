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

package com.ibm.crail.storage.reflex;

import com.ibm.crail.conf.CrailConfiguration;
import com.ibm.crail.metadata.DataNodeInfo;
import com.ibm.crail.storage.StorageEndpoint;
import com.ibm.crail.storage.StorageServer;
import com.ibm.crail.storage.reflex.client.ReFlexStorageEndpoint;
import com.ibm.crail.storage.reflex.client.ReflexStorageClient;
import com.ibm.crail.utils.CrailUtils;
import com.ibm.crail.storage.StorageTier;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import stanford.mast.reflex.ReFlexEndpointGroup;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;

public class ReFlexStorageTier extends ReflexStorageClient implements StorageTier{

	@Override
	public StorageServer launchServer() throws Exception {
		return new ReFlexStorageServer();
	}
}
