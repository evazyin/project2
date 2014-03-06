package project2;
import java.io.*;
import java.net.*;

public class NodeAddress implements Serializable {

	private final InetAddress address;
	private final int port;
	
	public NodeAddress(InetAddress address, int port) {
		this.address = address;
		this.port = port;
	}
	
	public InetAddress getAddress() {
		return address;
	}
	
	public int getPort() {
		return port;
	}
	
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof NodeAddress))
            return false;

        NodeAddress addr = (NodeAddress)obj;
        return this.address.equals(addr.address) && this.port == addr.port;
    }
    
    public int hashCode() {
        return address.hashCode() + new Integer(port).hashCode();
    }
}
