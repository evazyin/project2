package project2;
import java.io.*;
import java.nio.*;

public class ProposerMessageFormatter {

	public RawMessage createPromiseMessage (
			int instanceId, int n, int lastAcceptedN, WallPost lastAcceptedV) throws IOException {
		
		ByteBuffer bb = ByteBuffer.allocate(1 + 3 * Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.put(MessageType.PRP_PROMISE);
		bb.putInt(instanceId);
		bb.putInt(n);
		bb.putInt(lastAcceptedN);
		byte[] nBuffer = bb.array();
		
		byte[] vBuffer = null;
		if (lastAcceptedV != null) {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutput oo = new ObjectOutputStream(baos);
			oo.writeObject(lastAcceptedV);
			vBuffer = baos.toByteArray();
		}
		
		byte[] buffer = new byte[nBuffer.length + (vBuffer != null? vBuffer.length : 0)];
		System.arraycopy(nBuffer, 0, buffer, 0, nBuffer.length);
		
		if (vBuffer != null) {
			System.arraycopy(vBuffer, 0, buffer, nBuffer.length, vBuffer.length);
		}
		
		return new RawMessage(buffer, buffer.length);
	}
	
	public RawMessage createAcceptedMessage(int instanceId, int n) {
		ByteBuffer bb = ByteBuffer.allocate(1 + 2 * Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.put(MessageType.PRP_ACCEPTED);
		bb.putInt(instanceId);
		bb.putInt(n);
		
		return new RawMessage(bb.array(), bb.array().length);
	}
	
	public RawMessage createNackMessage(int instanceId, int n) {
		ByteBuffer bb = ByteBuffer.allocate(1 + 2 * Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.put(MessageType.PRP_NACK);
		bb.putInt(instanceId);
		bb.putInt(n);
		
		return new RawMessage(bb.array(), bb.array().length);
	}
}
