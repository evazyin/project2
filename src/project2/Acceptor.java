package project2;

import java.io.IOException;

public class Acceptor {

	private final int instanceNo;
	private final AcceptorSender sender;
	private final ILog log;
	
	private int nMax;
	private int lastAcceptedN;
	private WallPost lastAcceptedV;
	
	public Acceptor(int instanceNo, AcceptorSender sender, ILog log) {
		this.instanceNo = instanceNo;
		this.sender = sender;
		this.log = log;
	}
	
	public void processPrepare(NodeAddress proposerAddr, int n) throws IOException {
		log.Log("Acceptor [" + instanceNo + "]: Received a prepare request from " + proposerAddr.getAddress() + ":" + proposerAddr.getPort()
				+ " (n = " + n + ").");
		
		if (n > nMax) {
			nMax = n;
			sender.sendPromise(proposerAddr, instanceNo, n, lastAcceptedN, lastAcceptedV);
		} else {
			sender.sendReject(proposerAddr, instanceNo, nMax);
		}
	}
	
	public void processAccept(NodeAddress proposerAddr, int n, WallPost v) throws IOException {
		log.Log("Acceptor [" + instanceNo + "]: Received an accept request from " + proposerAddr.getAddress() + ":" + proposerAddr.getPort()
				+ " (n = " + n + ").");

		if (nMax > n) {
			sender.sendReject(proposerAddr, instanceNo, nMax);
		} else {
			nMax = n;
			lastAcceptedN = n;
			lastAcceptedV = v;
			sender.sendAccept(proposerAddr, instanceNo, n, v);
		}
	}
}
