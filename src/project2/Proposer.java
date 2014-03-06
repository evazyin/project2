package project2;
import java.io.*;
import java.net.*;
import java.nio.*;
import java.nio.charset.Charset;
import java.util.*;

public class Proposer extends Thread {

	private static final int INPUT_BUFFER_SIZE = 65500;
	
	private final int nodeNo;
	private final int port;
	private final Collection<NodeAddress> acceptors;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates = new HashMap<Integer, InstanceState>();
	
	private DatagramSocket socket;
	
	private int lastInstanceNo = 0;

	public Proposer(int nodeNo, int port, Collection<NodeAddress> acceptors, ILog log) {
		this.nodeNo = nodeNo;
		this.port = port;
		this.acceptors = acceptors;
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
	
	protected void dispatch(RawMessage message, NodeAddress address)
			throws IOException, ClassNotFoundException {
		switch (message.getData()[0]) {
			case MessageType.CLIENT_REQUEST:
			{
				byte[] buffer = new byte[message.getLength() - 1];
				System.arraycopy(message.getData(), 1, buffer, 0, message.getLength() - 1);
				processClientRequest(address, new String(buffer, Charset.forName("US-ASCII")));
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
				
				processPromise(address, bb.getInt(), bb.getInt(), bb.getInt(), v);
				break;
			}
			case MessageType.PRP_ACCEPTED:
			{
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, 2 * Integer.SIZE / Byte.SIZE);
				bb.order(ByteOrder.BIG_ENDIAN);
				processAccept(address, bb.getInt(), bb.getInt());
				break;
			}
			case MessageType.PRP_NACK:
			{
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, 2 * Integer.SIZE / Byte.SIZE);
				bb.order(ByteOrder.BIG_ENDIAN);
				processNack(address, bb.getInt(), bb.getInt());
				break;
			}
			default:
				log.LogError("Proposer: Received a message of unrecognized type from "
						+ address.getAddress() + ":" + address.getPort());
		}
	}
	
	protected void processClientRequest(NodeAddress client, String message) throws IOException {
		log.Log("Proposer: Received a request from client " + client.getAddress() + ":" + client.getPort()
				+ " to post message \"" + message + "\".");
		
		initiateNewInstance(new WallPost(client, message));
	}
	
	protected void processPromise(
			NodeAddress acceptor, int instanceNo, int n, int lastAcceptedN, WallPost lastAcceptedV)
			throws IOException {
		log.Log("Proposer: Received a promise from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		instanceState.promises.add(acceptor);
		if (lastAcceptedN > instanceState.acceptedNMax) {
			instanceState.acceptedNMax = lastAcceptedN;
			instanceState.valueForAcceptedNMax = lastAcceptedV;
		}
		
		if (promisedByQuorum(instanceState) && !instanceState.acceptSent()) {
			boolean isCommittingOriginalValue = instanceState.valueForAcceptedNMax == null;
			WallPost vToCommit = isCommittingOriginalValue?
					instanceState.originalValue : instanceState.valueForAcceptedNMax;
			sendAcceptMessageToAcceptors(instanceNo, n, vToCommit);
			instanceState.valueCommitted = vToCommit;
			
			if (!isCommittingOriginalValue) {
				initiateNewInstance(instanceState.originalValue);
			}
		}
	}
	
	protected void processAccept(NodeAddress acceptor, int instanceNo, int n) {
		log.Log("Proposer: Received an accept from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		instanceState.accepts.add(acceptor);
		
		if (acceptedByQuorum(instanceState)) {
			sendNotificationToClient(instanceNo);
		}
	}
	
	protected void processNack(NodeAddress acceptor, int instanceNo, int n) throws IOException {
		log.Log("Proposer: Received a reject from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		if (!instanceState.abandoned) {
			initiateNewInstance(instanceState.acceptSent()?
					instanceState.valueCommitted : instanceState.originalValue);
			instanceState.abandoned = true;
		}
	}
	
	protected void sendPrepareMessageToAcceptors(int instanceNo, int n) throws IOException {
		log.Log("Proposer: Sending prepare message to acceptors for instance no "
				+ instanceNo + " (n = " + n + ").");

		AcceptorMessageFormatter formatter = new AcceptorMessageFormatter();
		RawMessage acceptorMessage = formatter.createPrepareMessage(instanceNo, n);
		
		for (NodeAddress acceptor : acceptors) {
			DatagramPacket packet = new DatagramPacket(
					acceptorMessage.getData(),
					acceptorMessage.getLength(),
					acceptor.getAddress(),
					acceptor.getPort());
			socket.send(packet);
		}
	}
	
	protected void sendAcceptMessageToAcceptors(int instanceNo, int n, WallPost v) throws IOException {
		log.Log("Proposer: Sending accept message to acceptors for instance no "
				+ instanceNo + " (n = " + n + "), message \"" + v.getMessage() + "\".");

		AcceptorMessageFormatter formatter = new AcceptorMessageFormatter();
		RawMessage acceptorMessage = formatter.createAcceptMessage(instanceNo, n, v);
		
		for (NodeAddress acc : instanceStates.get(instanceNo).promises) {
			DatagramPacket packet = new DatagramPacket(
					acceptorMessage.getData(),
					acceptorMessage.getLength(),
					acc.getAddress(),
					acc.getPort());
			socket.send(packet);
		}
	}
	
	protected void sendNotificationToClient(int instanceNo) {
		log.Log("Proposer: Sending a notification to client for instance no "
				+ instanceNo + ".");
		
		// TODO: implement
	}
	
	private void initiateNewInstance(WallPost v) throws IOException {
		int instanceNo = getLastInstanceNo() + 1;
		InstanceState instanceState = createInstance(v);
		instanceStates.put(instanceNo, instanceState);
		sendPrepareMessageToAcceptors(instanceNo, nodeNo);
	}
	
	private InstanceState createInstance(WallPost v) {
		InstanceState instanceState = new InstanceState();
		instanceState.originalValue = v;
		return instanceState;
	}
	
	protected int getLastInstanceNo() {
		// TODO: get from learner
		return lastInstanceNo++;
	}
	
	private boolean promisedByQuorum(InstanceState instanceState) {
		return instanceState.promises.size() > acceptors.size() / 2;
	}
	
	private boolean acceptedByQuorum(InstanceState instanceState) {
		return instanceState.accepts.size() > acceptors.size() / 2;
	}
	
	private class InstanceState {
		public HashSet<NodeAddress> promises = new HashSet<NodeAddress>();
		public HashSet<NodeAddress> accepts = new HashSet<NodeAddress>();
		public WallPost originalValue;
		public int acceptedNMax;
		public WallPost valueForAcceptedNMax;
		public WallPost valueCommitted;
		public boolean abandoned;
		
		public boolean acceptSent() {
			return valueCommitted != null;
		}
	}
}
