package project2;
import java.io.*;
import java.net.*;
import java.util.*;

public class Main {

	public static void main(String[] args) throws Exception {
		
		int nodeNumber = Integer.parseInt(args[0]);
		int port = Integer.parseInt(args[1]);
		
		BufferedReader reader = new BufferedReader(new FileReader(args[2]));
		ArrayList<NodeAddress> acceptors = new ArrayList<NodeAddress>();
		ArrayList<NodeAddress> learners = new ArrayList<NodeAddress>();
		
		String line;
		boolean reachedLearners = false;
		while ((line = reader.readLine()) != null) {
			if (line.equals("")) {
				reachedLearners = true;
				continue;
			}
			
			String[] addrAndPort = line.split("\t");
			InetAddress addr = InetAddress.getByName(addrAndPort[0]);
			int nodePort = Integer.parseInt(addrAndPort[1]);
			(reachedLearners? learners : acceptors).add(new NodeAddress(addr, nodePort));
		}
		
		reader.close();
		
		ILog log = new StandardLog();
		
		new Proposer(nodeNumber, port, acceptors, log).start();
		new Acceptor(acceptors.get(nodeNumber - 1).getPort(), learners, log).start();
		new Learner(
				learners.get(nodeNumber - 1).getPort(),
				(int)(Math.floor((double)acceptors.size() / 2.0) + 1),
				log).start();
	}
}