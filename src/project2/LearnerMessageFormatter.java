package project2;

import java.io.*;
import java.nio.*;

public class LearnerMessageFormatter {

	public RawMessage createAcceptedMessage(int instanceNo, WallPost v) throws IOException {
		ByteBuffer bb = ByteBuffer.allocate(1 + Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.put(MessageType.LRN_ACCEPTED);
		bb.putInt(instanceNo);
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
