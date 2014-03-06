package project2;
import java.io.*;
import java.nio.*;

public class AcceptorMessageFormatter {

	public RawMessage createPrepareMessage(int instanceNo, int n) {
		ByteBuffer bb = ByteBuffer.allocate(1 + 2 * Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.put(MessageType.ACC_PREPARE);
		bb.putInt(instanceNo);
		bb.putInt(n);
		
		return new RawMessage(bb.array(), bb.array().length);
	}
	
	public RawMessage createAcceptMessage(int instanceNo, int n, WallPost v) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(1 + 2 * Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.put(MessageType.ACC_ACCEPT);
		bb.putInt(instanceNo);
		bb.putInt(n);
		byte[] nBuffer = bb.array();
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ObjectOutput oo = new ObjectOutputStream(baos);
		oo.writeObject(v);
		byte[] vBuffer = baos.toByteArray();
		
		byte[] buffer = new byte[nBuffer.length + vBuffer.length];
		System.arraycopy(nBuffer, 0, buffer, 0, nBuffer.length);
		System.arraycopy(vBuffer, 0, buffer, nBuffer.length, vBuffer.length);

		return new RawMessage(buffer, buffer.length);
	}
}
