package project2;

import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;

public class AcceptorListener extends Thread {

	private static final int MIN_WORKING_TIME = 5;
	private static final int MAX_WORKING_TIME = 15;
	private static final int MIN_FAILURE_TIME = 2;
	private static final int MAX_FAILURE_TIME = 5;
	
	private static final int INPUT_BUFFER_SIZE = 65500;
	
	private final HashMap<Integer, Acceptor> acceptors = new HashMap<Integer, Acceptor>();
	
	private final AcceptorSender acceptorSender;
	private final DatagramSocket socket;
	private final ILog log;
	
	private boolean iPretendToHaveFailed;
	private Timer timer;
	private Random random;
	
	public AcceptorListener(AcceptorSender acceptorSender, DatagramSocket socket, ILog log) {
		this.acceptorSender = acceptorSender;
		this.socket = socket;
		this.log = log;
	}
	
	public void run() {
		timer = new Timer();
		random = new Random();
		beginNormalOperation();
		while (true) {
			byte[] bufferRx = new byte[INPUT_BUFFER_SIZE];
			DatagramPacket packetRx = new DatagramPacket(bufferRx, bufferRx.length);
			try {
				socket.receive(packetRx);
				if (!iPretendToHaveFailed) {
					dispatch(
							new RawMessage(packetRx.getData(), packetRx.getLength()),
							new NodeAddress(packetRx.getAddress(), packetRx.getPort()));
				}
			} catch (IOException e) {
				log.LogError(e.getMessage());
			} catch (ClassNotFoundException e) {
				log.LogError(e.getMessage());
			}
		}
	}
	
	protected void dispatch(RawMessage message, NodeAddress proposerAddr)
			throws IOException, ClassNotFoundException {
		switch (message.getData()[0]) {
			case MessageType.ACC_PREPARE:
			{
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, 2 * Integer.SIZE / Byte.SIZE);
				bb.order(ByteOrder.BIG_ENDIAN);
				int instanceNo = bb.getInt();
				int n = bb.getInt();
				
				Acceptor acceptor;
				if (acceptors.containsKey(instanceNo)) {
					acceptor = acceptors.get(instanceNo);
				} else {
					acceptor = new Acceptor(instanceNo, acceptorSender, log);
					acceptors.put(instanceNo, acceptor);
				}
				
				acceptor.processPrepare(proposerAddr, n);
				break;
			}
			case MessageType.ACC_ACCEPT:
			{
				int fixedPartSize = 2 * Integer.SIZE / Byte.SIZE;
				ByteBuffer bb = ByteBuffer.wrap(message.getData(), 1, fixedPartSize);
				bb.order(ByteOrder.BIG_ENDIAN);
				int instanceNo = bb.getInt();
				int n = bb.getInt();
				
				int valueOffset = 1 + fixedPartSize;
				ByteArrayInputStream bais = new ByteArrayInputStream(
						message.getData(), valueOffset, message.getLength() - valueOffset);
				ObjectInputStream ois = new ObjectInputStream(bais);
				WallPost v = (WallPost)ois.readObject();
				
				Acceptor acceptor;
				if (acceptors.containsKey(instanceNo)) {
					acceptor = acceptors.get(instanceNo);
				} else {
					acceptor = new Acceptor(instanceNo, acceptorSender, log);
					acceptors.put(instanceNo, acceptor);
				}

				acceptor.processAccept(proposerAddr, n, v);
				break;
			}
			default:
				log.LogError("Acceptor: Received a message of unrecognized type from "
						+ proposerAddr.getAddress() + ":" + proposerAddr.getPort());
		}
	}
	
	private void beginNormalOperation() {
		iPretendToHaveFailed = false;
		log.Log("Acceptor: Started up.");
		int length = 1000 * (random.nextInt(MAX_WORKING_TIME - MIN_WORKING_TIME + 1) + MIN_WORKING_TIME);
		timer.schedule(new TimerTask() {
			public void run() {
				beginFailurePeriod();
			}
		}, length);
	}
	
	private void beginFailurePeriod() {
		iPretendToHaveFailed = true;
		log.Log("Acceptor: Shutting down (simulated failure).");
		int length = 1000 * (random.nextInt(MAX_FAILURE_TIME - MIN_FAILURE_TIME + 1) + MIN_FAILURE_TIME);
		timer.schedule(new TimerTask() {
			public void run() {
				beginNormalOperation();
			}
		}, length);
	}
}
