package project2;

import java.io.*;
import java.net.*;
import java.util.*;

public class AcceptorSender {

	private final Collection<NodeAddress> learners;
	private final DatagramSocket socket;
	private final ILog log;
	
	public AcceptorSender(Collection<NodeAddress> learners, DatagramSocket socket, ILog log) {
		this.learners = learners;
		this.socket = socket;
		this.log = log;
	}
	
	public void sendPromise(
			NodeAddress proposerAddr,
			int instanceNo,
			int n,
			int lastAcceptedN,
			WallPost lastAcceptedV) throws IOException {
		log.Log("Acceptor [" + instanceNo + "]: Sending a promise (n = " + n + "). " + (lastAcceptedN > 0?
					"Last accepted n = " + lastAcceptedN + ", last value = " + lastAcceptedV.toString() + "."
					: "No values accepted before."));
		
		ProposerMessageFormatter formatter = new ProposerMessageFormatter();
		RawMessage message = formatter.createPromiseMessage(instanceNo, n, lastAcceptedN, lastAcceptedV);
		DatagramPacket packet = new DatagramPacket(
				message.getData(), message.getLength(), proposerAddr.getAddress(), proposerAddr.getPort());
		socket.send(packet);
	}
	
	public void sendReject(NodeAddress proposerAddr, int instanceNo, int n) throws IOException {
		log.Log("Acceptor [" + instanceNo + "]: Rejecting a request (n = " + n + "). ");
		
		ProposerMessageFormatter formatter = new ProposerMessageFormatter();
		RawMessage message = formatter.createNackMessage(instanceNo, n);
		DatagramPacket packet = new DatagramPacket(
				message.getData(), message.getLength(), proposerAddr.getAddress(), proposerAddr.getPort());
		socket.send(packet);
	}
	
	public void sendAccept(NodeAddress proposerAddr, int instanceNo, int n, WallPost v)
			throws IOException {
		log.Log("Acceptor [" + instanceNo + "]: Accepting a request (n = " + n + "). ");
		
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
}
