package project2;

import java.io.*;
import java.net.*;
import java.nio.*;

public class LearnerListener extends Thread {

	private static final int INPUT_BUFFER_SIZE = 65500;
	
	private final Learner learner;
	private final DatagramSocket socket;
	private final ILog log;

	public LearnerListener(Learner learner, DatagramSocket socket, ILog log) {
		this.learner = learner;
		this.socket = socket;
		this.log = log;
	}
	
	public void run() {
			while (true) {
				byte[] bufferRx = new byte[INPUT_BUFFER_SIZE];
				DatagramPacket packetRx = new DatagramPacket(bufferRx, bufferRx.length);
				try {
					socket.receive(packetRx);
					dispatch(
							new RawMessage(packetRx.getData(), packetRx.getLength()),
							new NodeAddress(packetRx.getAddress(), packetRx.getPort()));
				} catch (IOException e) {
					log.LogError(e.getMessage());
				}catch (ClassNotFoundException e) {
					log.LogError(e.getMessage());
				}
			}
	}
	
	protected void dispatch(RawMessage message, NodeAddress acceptorAddr)
			throws IOException, ClassNotFoundException {
		switch (message.getData()[0]) {
			case MessageType.LRN_ACCEPTED:
			{
				int fixedPartSize = Integer.SIZE / Byte.SIZE;
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, fixedPartSize);
				bb.order(ByteOrder.BIG_ENDIAN);
				
				int valueOffset = 1 + fixedPartSize;
				ByteArrayInputStream bais = new ByteArrayInputStream(
						message.getData(), valueOffset, message.getLength() - valueOffset);
				ObjectInputStream ois = new ObjectInputStream(bais);
				WallPost v = (WallPost)ois.readObject();
				
				learner.processAccepted(acceptorAddr, bb.getInt(), v);
				break;
			}
			default:
				log.LogError("Learner: Received a message of unrecognized type from "
						+ acceptorAddr.getAddress() + ":" + acceptorAddr.getPort());
		}
	}
}
