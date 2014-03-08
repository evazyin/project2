package project2;

import java.io.*;
import java.util.*;

public class Proposer {
	
	private final int nodeNo;
	private final ProposerSender proposerSender;
	private final Collection<NodeAddress> acceptors;
	private final InstanceNumberProvider instanceNumberProvider;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates = new HashMap<Integer, InstanceState>();
	
	public Proposer(
			int nodeNo,
			ProposerSender proposerSender,
			Collection<NodeAddress> acceptors,
			InstanceNumberProvider instanceNumberProvider,
			ILog log) {
		this.nodeNo = nodeNo;
		this.proposerSender = proposerSender;
		this.acceptors = acceptors;
		this.instanceNumberProvider = instanceNumberProvider;
		this.log = log;
	}
		
	public void processClientRequest(NodeAddress client, String message) throws IOException {
		log.Log("Proposer: Received a request from client " + client.getAddress() + ":" + client.getPort()
				+ " to post message \"" + message + "\".");
		
		initiateNewInstance(new WallPost(client, message));
	}
	
	public void processPromise(
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
			proposerSender.sendAcceptMessageToAcceptors(
					instanceNo, n, vToCommit, instanceStates.get(instanceNo).promises);
			instanceState.valueCommitted = vToCommit;
			
			if (!isCommittingOriginalValue) {
				initiateNewInstance(instanceState.originalValue);
			}
		}
	}
	
	public void processAccept(NodeAddress acceptor, int instanceNo, int n) throws IOException {
		log.Log("Proposer: Received an accept from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		instanceState.accepts.add(acceptor);
		
		if (acceptedByQuorum(instanceState)) {
			proposerSender.sendNotificationToClient(instanceNo, instanceState.valueCommitted.getOrigin());
		}
	}
	
	public void processNack(NodeAddress acceptor, int instanceNo, int n) throws IOException {
		log.Log("Proposer: Received a reject from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		if (!instanceState.abandoned) {
			initiateNewInstance(instanceState.acceptSent()?
					instanceState.valueCommitted : instanceState.originalValue);
			instanceState.abandoned = true;
		}
	}
	
	
	private void initiateNewInstance(WallPost v) throws IOException {
		int instanceNo = instanceNumberProvider.getLastInstanceNo() + 1;
		InstanceState instanceState = createInstance(v);
		instanceStates.put(instanceNo, instanceState);
		proposerSender.sendPrepareMessageToAcceptors(instanceNo, nodeNo, acceptors);
	}
	
	private InstanceState createInstance(WallPost v) {
		InstanceState instanceState = new InstanceState();
		instanceState.originalValue = v;
		return instanceState;
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
