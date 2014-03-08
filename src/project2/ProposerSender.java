package project2;

import java.io.*;
import java.net.*;

public class ProposerSender {

	private final DatagramSocket socket;
	private final ILog log;
	
	public ProposerSender(DatagramSocket socket, ILog log) {
		this.socket = socket;
		this.log = log;
	}
	
	public void sendPrepareMessageToAcceptors(
			int instanceNo, int n, Iterable<NodeAddress> acceptors) throws IOException {
		log.Log("Proposer [" + instanceNo + "]: Sending prepare message to acceptors (n = " + n + ").");

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
	
	public void sendAcceptMessageToAcceptors(
			int instanceNo, int n, WallPost v, Iterable<NodeAddress> acceptors) throws IOException {
		log.Log("Proposer [" + instanceNo + "]: Sending accept message to acceptors (n = "
				+ n + "), message \"" + v.getMessage() + "\".");

		AcceptorMessageFormatter formatter = new AcceptorMessageFormatter();
		RawMessage acceptorMessage = formatter.createAcceptMessage(instanceNo, n, v);
		
		for (NodeAddress acc : acceptors) {
			DatagramPacket packet = new DatagramPacket(
					acceptorMessage.getData(),
					acceptorMessage.getLength(),
					acc.getAddress(),
					acc.getPort());
			socket.send(packet);
		}
	}
	
	public void sendNotificationToClient(int instanceNo, NodeAddress client) throws IOException {
		log.Log("Proposer [" + instanceNo + "]: Sending a notification to client.");
		
		RawMessage message = new ClientMessageFormatter().createConfirmationMessage(instanceNo);
		DatagramPacket packet = new DatagramPacket(
				message.getData(),
				message.getLength(),
				client.getAddress(),
				client.getPort());
		socket.send(packet);
	}
	
	public void sendFailureNotificationToClient(int instanceNo, NodeAddress client) throws IOException {
		log.Log("Proposer [" + instanceNo + "]: Sending a failure notification to client.");
		
		RawMessage message = new ClientMessageFormatter().createFailureMessage();
		DatagramPacket packet = new DatagramPacket(
				message.getData(),
				message.getLength(),
				client.getAddress(),
				client.getPort());
		socket.send(packet);
	}
}
