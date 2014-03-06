package project2;
import java.io.IOException;
import java.net.*;
import java.nio.charset.Charset;

public class Client {

	public static void main(String[] args) {
		if (args.length < 3) {
			displayUsageAndExit();
		}

		try {
			InetAddress serverAddr = InetAddress.getByName(args[0]);
			int port = Integer.parseInt(args[1]);
			sendRequest(serverAddr, port, args[2], new StandardLog());
		} catch (UnknownHostException e) {
			System.err.println(e.getMessage());
			displayUsageAndExit();
		} catch (NumberFormatException e) {
			System.err.println("Invalid port number.");
			displayUsageAndExit();
		}
	}
	
	public static void sendRequest(InetAddress serverAddr, int port, String message, ILog log) {
		try {
			DatagramSocket socket = new DatagramSocket();
			log.Log("Posting message: \"" + message + "\"");
			
			byte[] messageAsBytes = message.getBytes(Charset.forName("US-ASCII"));
			byte[] buffer = new byte[messageAsBytes.length + 1];
			buffer[0] = MessageType.CLIENT_REQUEST;
			System.arraycopy(messageAsBytes, 0, buffer, 1, messageAsBytes.length);
			DatagramPacket packetTx = new DatagramPacket(buffer, buffer.length, serverAddr, port);
			socket.send(packetTx);
			socket.close();
		} catch (SocketException e) {
			log.LogError(e.getMessage());
		} catch (IOException e) {
			log.LogError(e.getMessage());
		}
	}
	
	private static void displayUsageAndExit() {
		System.err.println("Usage: java Client <server address> <port> <message>");
	    System.exit(1);
	}	
}
