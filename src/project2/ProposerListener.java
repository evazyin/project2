package project2;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.*;

public class ProposerListener extends Thread {

	private static final int INPUT_BUFFER_SIZE = 65500;
	
	private final Proposer proposer;
	private final DatagramSocket socket;
	private final ILog log;
	
	public ProposerListener(Proposer proposer, DatagramSocket socket, ILog log) {
		this.proposer = proposer;
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
			} catch (ClassNotFoundException e) {
				log.LogError(e.getMessage());
			}
		}
	}
	
	protected void dispatch(RawMessage message, NodeAddress address)
			throws IOException, ClassNotFoundException {
		switch (message.getData()[0]) {
			case MessageType.CLIENT_REQUEST:
			{
				byte[] buffer = new byte[message.getLength() - 1];
				System.arraycopy(message.getData(), 1, buffer, 0, message.getLength() - 1);
				proposer.processClientRequest(address, new String(buffer, Charset.forName("US-ASCII")));
				break;
			}
			case MessageType.PRP_PROMISE:
			{
				int fixedPartSize = 3 * Integer.SIZE / Byte.SIZE;
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, fixedPartSize);
				bb.order(ByteOrder.BIG_ENDIAN);
				
				WallPost v = null;
				int valueOffset = 1 + fixedPartSize;
				if (message.getLength() > valueOffset) {
					ByteArrayInputStream bais = new ByteArrayInputStream(
							message.getData(), valueOffset, message.getLength() - valueOffset);
					ObjectInputStream ois = new ObjectInputStream(bais);
					v = (WallPost)ois.readObject();
				}
				
				proposer.processPromise(address, bb.getInt(), bb.getInt(), bb.getInt(), v);
				break;
			}
			case MessageType.PRP_ACCEPTED:
			{
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, 2 * Integer.SIZE / Byte.SIZE);
				bb.order(ByteOrder.BIG_ENDIAN);
				proposer.processAccept(address, bb.getInt(), bb.getInt());
				break;
			}
			case MessageType.PRP_NACK:
			{
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, 2 * Integer.SIZE / Byte.SIZE);
				bb.order(ByteOrder.BIG_ENDIAN);
				proposer.processNack(address, bb.getInt(), bb.getInt());
				break;
			}
			default:
				log.LogError("Proposer: Received a message of unrecognized type from "
						+ address.getAddress() + ":" + address.getPort());
		}
	}
}
