package project2;

import java.io.*;
import java.util.*;

public class Proposer {
	
	private final int TIMEOUT = 2000;
	private final int MAX_RETRIES = 3;
	
	private final int nodeNo;
	private final ProposerSender proposerSender;
	private final Collection<NodeAddress> acceptors;
	private final InstanceNumberProvider instanceNumberProvider;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates = new HashMap<Integer, InstanceState>();
	private int lastInstanceNo;
	
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
		log.Log("Proposer [" + instanceNo + "]: Received a promise from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		instanceState.promises.add(acceptor);
		if (lastAcceptedN > instanceState.acceptedNMax) {
			instanceState.acceptedNMax = lastAcceptedN;
			instanceState.valueForAcceptedNMax = lastAcceptedV;
		}
		
		if (promisedByQuorum(instanceState)
				&& !instanceState.acceptSent()
				&& !instanceState.abandoned) {
			
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
		log.Log("Proposer [" + instanceNo + "]: Received an accept from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		instanceState.accepts.add(acceptor);
		
		if (acceptedByQuorum(instanceState) && !instanceState.abandoned) {
			instanceState.timer.cancel();
			proposerSender.sendNotificationToClient(instanceNo, instanceState.valueCommitted.getOrigin());
		}
	}
	
	public void processNack(NodeAddress acceptor, int instanceNo, int n) throws IOException {
		log.Log("Proposer [" + instanceNo + "]: Received a reject from acceptor " + acceptor.getAddress() + ":" + acceptor.getPort()
				+ " (n = " + n + ").");
		
		InstanceState instanceState = instanceStates.get(instanceNo);
		if (!instanceState.abandoned) {
			instanceState.timer.cancel();
			instanceState.abandoned = true;
			initiateNewInstance(instanceState.acceptSent()?
					instanceState.valueCommitted : instanceState.originalValue);
		}
	}
	
	
	private void initiateNewInstance(WallPost v) throws IOException {
		int instanceNo = getNewInstanceNo();
		InstanceState instanceState = createInstance(v);
		instanceState.lastNSent = nodeNo;
		instanceState.timer = new Timer();
		instanceState.timer.schedule(new TimeoutTask(instanceNo), TIMEOUT);
		instanceStates.put(instanceNo, instanceState);
		proposerSender.sendPrepareMessageToAcceptors(instanceNo, nodeNo, acceptors);
	}
	
	private class TimeoutTask extends TimerTask {
		private final int instanceNo;
		
		public TimeoutTask(int instanceNo) {
			this.instanceNo = instanceNo;
		}
		
		public void run() {
			handleTimeout(instanceNo);
		}
	}
	
	private InstanceState createInstance(WallPost v) {
		InstanceState instanceState = new InstanceState();
		instanceState.originalValue = v;
		return instanceState;
	}
	
	private void handleTimeout(int instanceNo) {
		log.Log("Proposer [" + instanceNo + "]: Request has timed out.");
		
		try {
			InstanceState instanceState = instanceStates.get(instanceNo);
			instanceState.retries++;
			if (instanceState.retries <= MAX_RETRIES) {
				instanceState.timer = new Timer();
				instanceState.timer.schedule(new TimeoutTask(instanceNo), TIMEOUT);
				instanceState.lastNSent += acceptors.size();
				proposerSender.sendPrepareMessageToAcceptors(instanceNo, instanceState.lastNSent, acceptors);
			} else {
				instanceState.abandoned = true;
				log.Log("Proposer [" + instanceNo + "]: Reached the maximum number of retries. Abandoning request.");
				NodeAddress client = (instanceState.valueForAcceptedNMax != null?
						instanceState.valueForAcceptedNMax : instanceState.originalValue).getOrigin();
				proposerSender.sendFailureNotificationToClient(instanceNo, client);
			}
		} catch (IOException e) {
			log.LogError(e.getMessage());
		}
	}
	
	private boolean promisedByQuorum(InstanceState instanceState) {
		return instanceState.promises.size() > acceptors.size() / 2;
	}
	
	private boolean acceptedByQuorum(InstanceState instanceState) {
		return instanceState.accepts.size() > acceptors.size() / 2;
	}
	
	private int getNewInstanceNo() {
		int maxInstanceNo = Math.max(instanceNumberProvider.getLastInstanceNo(), lastInstanceNo);
		lastInstanceNo = maxInstanceNo + 1;
		return lastInstanceNo;
	}
	
	private class InstanceState {
		public HashSet<NodeAddress> promises = new HashSet<NodeAddress>();
		public HashSet<NodeAddress> accepts = new HashSet<NodeAddress>();
		public WallPost originalValue;
		public int acceptedNMax;
		public WallPost valueForAcceptedNMax;
		public WallPost valueCommitted;
		public boolean abandoned;
		public int lastNSent;
		public int retries;
		public Timer timer;
		
		public boolean acceptSent() {
			return valueCommitted != null;
		}
	}
}
