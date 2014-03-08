package project2;

import java.nio.*;

public class ClientMessageFormatter {

	public RawMessage createConfirmationMessage(int instanceNo) {
		ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.putInt(instanceNo);
		
		return new RawMessage(bb.array(), bb.array().length);
	}
	
	public RawMessage createFailureMessage() {
		ByteBuffer bb = ByteBuffer.allocate(Integer.SIZE / Byte.SIZE);
		bb.order(ByteOrder.BIG_ENDIAN);
		bb.putInt(0);
		
		return new RawMessage(bb.array(), bb.array().length);
	}
}
