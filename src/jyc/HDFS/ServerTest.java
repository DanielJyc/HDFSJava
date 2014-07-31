package jyc.HDFS;

import java.net.*;
import java.io.*;

public class ServerTest extends Thread {
	private Socket client;

	public ServerTest(Socket c) {
		this.client = c;
	}

	public void run() {
		try {
//			BufferedReader in = new BufferedReader(new InputStreamReader(
//					client.getInputStream()));
//			PrintWriter out = new PrintWriter(client.getOutputStream());
			ObjectOutputStream out = new ObjectOutputStream(client.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(client.getInputStream());
			// Mutil User but can't parallel
			String [] arr= (String[]) in.readObject();
			System.out.println(arr[0]+arr[1]);
			String[] s =new String[]{"123","232"};
			out.writeObject(s);
			out.flush();
			client.close();
		} catch (IOException ex) {
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

		}
	}

	public static void main(String[] args) throws IOException {
		ServerSocket server = new ServerSocket(5678);
		while (true) {
			// transfer location change Single User or Multi User
			ServerTest mu = new ServerTest(server.accept());
			mu.start();
		}
	}
}