package project2;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public class Acceptor extends Thread {

	private static final int INPUT_BUFFER_SIZE = 65500;
	
	private final int port;
	private final Collection<NodeAddress> learners;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates =
			new HashMap<Integer, InstanceState>();
	
	private DatagramSocket socket;
	
	public Acceptor(int port, Collection<NodeAddress> learners, ILog log) {
		this.port = port;
		this.learners = learners;
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
	
	protected void dispatch(RawMessage message, NodeAddress proposerAddr)
			throws IOException, ClassNotFoundException {
		switch (message.getData()[0]) {
			case MessageType.ACC_PREPARE:
			{
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, 2 * Integer.SIZE / Byte.SIZE);
				bb.order(ByteOrder.BIG_ENDIAN);
				processPrepare(proposerAddr, bb.getInt(), bb.getInt());
				break;
			}
			case MessageType.ACC_ACCEPT:
			{
				int fixedPartSize = 2 * Integer.SIZE / Byte.SIZE;
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, fixedPartSize);
				bb.order(ByteOrder.BIG_ENDIAN);
				
				int valueOffset = 1 + fixedPartSize;
				ByteArrayInputStream bais = new ByteArrayInputStream(
						message.getData(), valueOffset, message.getLength() - valueOffset);
				ObjectInputStream ois = new ObjectInputStream(bais);
				WallPost v = (WallPost)ois.readObject();
				
				processAccept(proposerAddr, bb.getInt(), bb.getInt(), v);
				break;
			}
			default:
				log.LogError("Acceptor: Received a message of unrecognized type from "
						+ proposerAddr.getAddress() + ":" + proposerAddr.getPort());
		}
	}
	
	protected void processPrepare(NodeAddress proposerAddr, int instanceNo, int n) throws IOException {
		log.Log("Acceptor: Received a prepare request from " + proposerAddr.getAddress() + ":" + proposerAddr.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		if (instanceStates.containsKey(instanceNo)) {
			InstanceState instanceState = instanceStates.get(instanceNo);
			if (n > instanceState.nMax) {
				instanceState.nMax = n;
				sendPromise(proposerAddr, instanceNo, n, instanceState.lastAcceptedN, instanceState.lastAcceptedV);
			} else {
				sendReject(proposerAddr, instanceNo, instanceState.nMax);
			}
		} else {
			InstanceState instanceState = new InstanceState();
			instanceState.nMax = n;
			instanceStates.put(instanceNo, instanceState);
			sendPromise(proposerAddr, instanceNo, n, 0, null);
		}
	}
	
	protected void processAccept(NodeAddress proposerAddr, int instanceNo, int n, WallPost v)
			throws IOException {
		log.Log("Acceptor: Received an accept request from " + proposerAddr.getAddress() + ":" + proposerAddr.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");

		InstanceState instanceState;
		if (instanceStates.containsKey(instanceNo)) {
			instanceState = instanceStates.get(instanceNo);
			if (instanceState.nMax > n) {
				sendReject(proposerAddr, instanceNo, instanceState.nMax);
				return;
			}
		} else {
			instanceState = new InstanceState();
			instanceStates.put(instanceNo, instanceState);
		}
		
		instanceState.nMax = n;
		instanceState.lastAcceptedN = n;
		instanceState.lastAcceptedV = v;
		sendAccept(proposerAddr, instanceNo, n, v);
	}
	
	protected void sendPromise(
			NodeAddress proposerAddr,
			int instanceNo,
			int n,
			int lastAcceptedN,
			WallPost lastAcceptedV) throws IOException {
		log.Log("Acceptor: Sending a promise for instance no " + instanceNo + ", n = " + n + ". "
				+ (lastAcceptedN > 0?
						"Last accepted n = " + lastAcceptedN + ", last value = " + lastAcceptedV.toString() + "."
						: "No values accepted before."));
		
		ProposerMessageFormatter formatter = new ProposerMessageFormatter();
		RawMessage message = formatter.createPromiseMessage(instanceNo, n, lastAcceptedN, lastAcceptedV);
		DatagramPacket packet = new DatagramPacket(
				message.getData(), message.getLength(), proposerAddr.getAddress(), proposerAddr.getPort());
		socket.send(packet);
	}
	
	protected void sendReject(NodeAddress proposerAddr, int instanceNo, int n) throws IOException {
		log.Log("Acceptor: Rejecting a request for instance no " + instanceNo + ", n = " + n + ". ");
		
		ProposerMessageFormatter formatter = new ProposerMessageFormatter();
		RawMessage message = formatter.createNackMessage(instanceNo, n);
		DatagramPacket packet = new DatagramPacket(
				message.getData(), message.getLength(), proposerAddr.getAddress(), proposerAddr.getPort());
		socket.send(packet);
	}
	
	protected void sendAccept(NodeAddress proposerAddr, int instanceNo, int n, WallPost v)
			throws IOException {
		log.Log("Acceptor: Accepting a request for instance no " + instanceNo + ", n = " + n + ". ");
		
		ProposerMessageFormatter proposerFormatter = new ProposerMessageFormatter();
		RawMessage proposerMessage = proposerFormatter.createAcceptedMessage(instanceNo, n);
		DatagramPacket proposerPacket = new DatagramPacket(
				proposerMessage.getData(), proposerMessage.getLength(), proposerAddr.getAddress(), proposerAddr.getPort());
		socket.send(proposerPacket);
		
		LearnerMessageFormatter learnerFormatter = new LearnerMessageFormatter();
		RawMessage learnerMessage = learnerFormatter.createAcceptedMessage(instanceNo, v);
		for (NodeAddress learner : learners) {
			DatagramPacket learnerPacket = new DatagramPacket(
					learnerMessage.getData(), learnerMessage.getLength(), learner.getAddress(), learner.getPort());
			socket.send(learnerPacket);
		}
	}
	
	private class InstanceState {
		public int nMax;
		public int lastAcceptedN;
		public WallPost lastAcceptedV;
	}
}
