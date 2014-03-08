package project2;

import java.util.*;

public class Learner implements InstanceNumberProvider {
	
	private final int quorumCount;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates =
			new HashMap<Integer, InstanceState>();
	
	private final HashMap<Integer, WallPost> journal = new HashMap<Integer, WallPost>();
	
	private int maxInstanceNo = 0;
	
	
	public Learner(int quorumSize, ILog log) {
		this.quorumCount = quorumSize;
		this.log = log;
	}
	
	public int getLastInstanceNo() {
		// this has the potential for a race condition but Paxos will ensure correctness
		return maxInstanceNo;
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
			if (instanceNo > maxInstanceNo) {
				maxInstanceNo = instanceNo;
			}
		}
	}
	
	private class InstanceState {
		public HashSet<NodeAddress> acceptors = new HashSet<NodeAddress>();
	}
}
