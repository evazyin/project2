package project2;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public class Learner extends Thread {
	
	private static final int INPUT_BUFFER_SIZE = 65500;
	
	private final int port;
	private final int quorumCount;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates =
			new HashMap<Integer, InstanceState>();
	
	private final HashMap<Integer, WallPost> journal = new HashMap<Integer, WallPost>();
	
	private DatagramSocket socket;
	
	public Learner(int port, int quorumSize, ILog log) {
		this.port = port;
		this.quorumCount = quorumSize;
		this.log = log;
	}
	
	public void run() {
		try {
			socket = new DatagramSocket(port);
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
				}
			}
		} catch (SocketException e) {
			log.LogError(e.getMessage());
		} catch (ClassNotFoundException e) {
			log.LogError(e.getMessage());
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
				
				processAccepted(acceptorAddr, bb.getInt(), v);
				break;
			}
			default:
				log.LogError("Learner: Received a message of unrecognized type from "
						+ acceptorAddr.getAddress() + ":" + acceptorAddr.getPort());
		}
	}
	
	protected void processAccepted(NodeAddress acceptorAddr, int instanceNo, WallPost v) {
		log.Log("Learner: Received an accept from " + acceptorAddr.getAddress() + ":" + acceptorAddr.getPort()
				+ " for instance no " + instanceNo + ".");
		
		if (instanceStates.containsKey(instanceNo)) {
			InstanceState instanceState = instanceStates.get(instanceNo);
			instanceState.acceptors.add(acceptorAddr);
			if (instanceState.acceptors.size() == quorumCount) {
				log.Log("Learner: A quorum of acceptors has agreed on value \""
						+ v.getMessage() + "\" for instance " + instanceNo + ".");
				journal.put(instanceNo, v);
			}
		} else {
			InstanceState instanceState = new InstanceState();
			instanceState.acceptors.add(acceptorAddr);
			instanceStates.put(instanceNo, instanceState);
		}
	}
	
	private class InstanceState {
		public HashSet<NodeAddress> acceptors = new HashSet<NodeAddress>();
	}
}
