/*
 * DiSNI: Direct Storage and Networking Interface
 *
 * Author: Jonas Pfefferle <jpf@zurich.ibm.com>
 *         Patrick Stuedi  <stu@zurich.ibm.com>
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

import java.nio.ByteBuffer;


// ReFlexEndpointGroup is the starting point for making a ReFlexEndpoint for each connection
// manage some common setup in here
public class ReFlexEndpointGroup {
	private ReFlexSetup reflex; 
    private final NativeDispatcher nativeDispatcher;
	
	public ReFlexEndpointGroup(String hugePath, long[] socketMemoryMB){
		this.nativeDispatcher = new NativeDispatcher();
		this.reflex = new ReFlexSetup(hugePath, socketMemoryMB, nativeDispatcher);
	}
	
	public ReFlexEndpointGroup(){
		this.nativeDispatcher = new NativeDispatcher();
		this.reflex = new ReFlexSetup(nativeDispatcher);
	}
	
	public ReFlexEndpoint createEndpoint(){
		return new ReFlexEndpoint(this, nativeDispatcher);
	}
	
	//--------------- internal ------------------

	public ByteBuffer allocateBuffer(int size, int alignment) {
		return reflex.allocateBuffer(size, alignment);
	}

	public void freeBuffer(ByteBuffer buffer) {
		reflex.freeBuffer(buffer);
	}
}
