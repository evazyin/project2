package project2;
import java.io.IOException;
import java.net.*;
import java.nio.*;
import java.nio.charset.Charset;

public class Client {

	private static final int QUERY_TIMEOUT = 5 * 1000;
	private static final int INPUT_BUFFER_SIZE = 65500;
	
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
			socket.setSoTimeout(QUERY_TIMEOUT);
			
			log.Log("Posting message: \"" + message + "\"...");
			byte[] messageAsBytes = message.getBytes(Charset.forName("US-ASCII"));
			byte[] bufferTx = new byte[messageAsBytes.length + 1];
			bufferTx[0] = MessageType.CLIENT_REQUEST;
			System.arraycopy(messageAsBytes, 0, bufferTx, 1, messageAsBytes.length);
			DatagramPacket packetTx = new DatagramPacket(bufferTx, bufferTx.length, serverAddr, port);
			socket.send(packetTx);
			
			byte[] bufferRx = new byte[INPUT_BUFFER_SIZE];
			DatagramPacket packetRx = new DatagramPacket(bufferRx, bufferRx.length);
			socket.receive(packetRx);
			ByteBuffer bb = ByteBuffer.wrap(packetRx.getData(), 0, packetRx.getLength());
			bb.order(ByteOrder.BIG_ENDIAN);
			log.Log("Success! Message saved as entry number " + bb.getInt() + ".");
			
			socket.close();
		} catch (SocketException e) {
			log.LogError(e.getMessage());
		} catch (SocketTimeoutException e) {
			log.LogError("Request timed out.");
		} catch (IOException e) {
			log.LogError(e.getMessage());
		}
	}
	
	private static void displayUsageAndExit() {
		System.err.println("Usage: java Client <server address> <port> <message>");
	    System.exit(1);
	}	
}
