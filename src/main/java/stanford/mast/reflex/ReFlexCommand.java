package stanford.mast.reflex;

import java.io.IOException;
import java.nio.ByteBuffer;

import stanford.mast.reflex.IOCompletion;
import sun.nio.ch.DirectBuffer;

public class ReFlexCommand {
	private final ReFlexEndpoint endpoint;
	private final NativeDispatcher nativeDispatcher;
	private ByteBuffer buffer;
	private long bufferAddress;
	private long linearBlockAddress;
	private int sectorCount;
	private final IOCompletion completion;
	private boolean isWrite;
	
	ReFlexCommand(ReFlexEndpoint endpoint,NativeDispatcher nativeDispatcher, ByteBuffer buffer, long linearBlockAddress,
				IOCompletion completion, boolean isWrite) {
		this.endpoint = endpoint;
		this.completion = completion;
		this.nativeDispatcher = nativeDispatcher;
		this.linearBlockAddress = linearBlockAddress;
		this.isWrite = isWrite;
		setBuffer(buffer);
	}


	ReFlexCommand(ReFlexEndpoint endpoint,NativeDispatcher nativeDispatcher, IOCompletion completion) {
		this.endpoint = endpoint;
		this.completion = completion;
		this.nativeDispatcher = nativeDispatcher;
	}
	
	public void execute() throws IOException {
		if (bufferAddress == 0) {
			throw new IllegalArgumentException("Buffer not set");
		}

		synchronized (endpoint) {
			if (!endpoint.isOpen()) {
				throw new IOException("Endpoint not open!");
			}
			if (isWrite) {
				//FIXME: do we also need to specify destination IP of ReFlex server or is endpoint already per target??
				write(bufferAddress, linearBlockAddress, sectorCount, completion);
			} else {
				//FIXME: do we also need to specify destination IP of ReFlex server??
				//System.out.format("reading lba %d size %d sectors\n", linearBlockAddress, sectorCount);
				read(bufferAddress, linearBlockAddress, sectorCount, completion);
			}
		}
	}

	//FIXME: either pass in NativeDispatcher or create one for inside ReFlexCommand class

	public void Op(long address, long linearBlockAddress, int count, IOCompletion completion, boolean write) throws IOException {
		try {
			completion.reset();
		} catch (PendingOperationException e) {
			throw new IllegalArgumentException("Completion not done", e);
		}
		completion.setCompletedArray(endpoint);
		int ret = nativeDispatcher._submit_io(address, linearBlockAddress, count, completion.address(), write);
		if (ret < 0) {
			throw new IOException("submit_io failed with " + ret);
		}
	}

	public void read(long address, long linearBlockAddress, int count, IOCompletion completion) throws IOException {
		Op(address, linearBlockAddress, count, completion, false);
	}

	public void write(long address, long linearBlockAddress, int count, IOCompletion completion) throws IOException {
		Op(address, linearBlockAddress, count, completion, true);
	}


	
	public boolean isDone(){
		return completion.done();
	}

	public boolean isPending() {  return completion.isPending(); }
	
	public void free() {
		completion.free();
	}

	public ReFlexCommand setLinearBlockAddress(long linearBlockAddress) {
		this.linearBlockAddress = linearBlockAddress;
		return this;
	}

	public ReFlexCommand setBuffer(ByteBuffer buffer) {
		this.sectorCount = buffer.remaining() / endpoint.getSectorSize();
		this.bufferAddress = ((DirectBuffer) buffer).address() + buffer.position();
		this.buffer = buffer;
		return this;
	}

	public ReFlexCommand read() {
		this.isWrite = false;
		return this;
	}

	public ReFlexCommand write() {
		this.isWrite = true;
		return this;
	}

	public ReFlexCommand setId(long id) {
		completion.setId(id);
		return this;
	}

	public long getId() {
		return completion.getId();
	}

	public IOCompletion getCompletion() {
		return completion;
	}
}
