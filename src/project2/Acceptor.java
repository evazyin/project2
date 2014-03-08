package project2;
import java.io.*;
import java.util.*;

public class Acceptor {

	private final AcceptorSender sender;
	private final ILog log;
	
	private final HashMap<Integer, InstanceState> instanceStates =
			new HashMap<Integer, InstanceState>();
	
	public Acceptor(AcceptorSender sender, ILog log) {
		this.sender = sender;
		this.log = log;
	}
	
	public void processPrepare(NodeAddress proposerAddr, int instanceNo, int n) throws IOException {
		log.Log("Acceptor: Received a prepare request from " + proposerAddr.getAddress() + ":" + proposerAddr.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");
		
		if (instanceStates.containsKey(instanceNo)) {
			InstanceState instanceState = instanceStates.get(instanceNo);
			if (n > instanceState.nMax) {
				instanceState.nMax = n;
				sender.sendPromise(proposerAddr, instanceNo, n, instanceState.lastAcceptedN, instanceState.lastAcceptedV);
			} else {
				sender.sendReject(proposerAddr, instanceNo, instanceState.nMax);
			}
		} else {
			InstanceState instanceState = new InstanceState();
			instanceState.nMax = n;
			instanceStates.put(instanceNo, instanceState);
			sender.sendPromise(proposerAddr, instanceNo, n, 0, null);
		}
	}
	
	public void processAccept(NodeAddress proposerAddr, int instanceNo, int n, WallPost v)
			throws IOException {
		log.Log("Acceptor: Received an accept request from " + proposerAddr.getAddress() + ":" + proposerAddr.getPort()
				+ " for instance no " + instanceNo + " (n = " + n + ").");

		InstanceState instanceState;
		if (instanceStates.containsKey(instanceNo)) {
			instanceState = instanceStates.get(instanceNo);
			if (instanceState.nMax > n) {
				sender.sendReject(proposerAddr, instanceNo, instanceState.nMax);
				return;
			}
		} else {
			instanceState = new InstanceState();
			instanceStates.put(instanceNo, instanceState);
		}
		
		instanceState.nMax = n;
		instanceState.lastAcceptedN = n;
		instanceState.lastAcceptedV = v;
		sender.sendAccept(proposerAddr, instanceNo, n, v);
	}
		
	private class InstanceState {
		public int nMax;
		public int lastAcceptedN;
		public WallPost lastAcceptedV;
	}
}
