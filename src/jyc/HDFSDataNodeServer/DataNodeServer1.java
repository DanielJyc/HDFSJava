package jyc.HDFSDataNodeServer;

import java.net.*;
import java.io.*;

public class DataNodeServer1 extends Thread {
	private Socket server;
	private String path ="."+ File.separator + "DataNode1" + File.separator;

	public DataNodeServer1(Socket ser) {
		this.server = ser;
		File dir = new File(path);
		if(dir.exists()){
			System.out.println("目录" + path + "存在");
		}
//		System.out.println(dir);
		dir.mkdirs();
	}

	public void run() {
		try {
			ObjectOutputStream out = new ObjectOutputStream(server.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(server.getInputStream());
			// 接收--Mutil User but can't parallel
			String [] arr= (String[]) in.readObject();
			System.out.println(arr[0]+arr[1]);
			switch (arr[0]) {
			case "write":
				System.out.println("Write command.");
				write(arr[1], arr[2]);
				break;
			case "read":				
				System.out.println("Read command.");
				String data = read(arr[1]); //从本地读取文件内容
				String[] s =new String[]{data};
				out.writeObject(s);  //发送
				out.flush();			
				break;
			case "delete":
				System.out.println("Read command.");
				String[] s1 = null;
				if(true == delete(arr[1])){		//从本地读取文件内容
					s1 =new String[]{"Delete done."};
				}
				else {
					s1 =new String[]{"Filename dose not exits."};
				}
				out.writeObject(s1);  //发送
				out.flush();
				break;
			default:
				System.out.println("Wrong command.");
				break;
			}
			out.close();
			in.close();
			server.close();
		} catch (IOException ex) {
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}

	private boolean delete(String chunk_uuid) {
		// TODO Auto-generated method stub
		File file = new File( path+chunk_uuid);
		if (!file.exists()) { // 不存在，返回false
			return false;
		} else {
			return file.delete();
		}
	}

	private String read(String chunk_uuid) throws IOException {
		// TODO Auto-generated method stub
		FileInputStream fis;
		try {
			fis = new FileInputStream(path+chunk_uuid);
			byte[] buf = new byte[fis.available()];// 定义一个刚刚好的缓冲区。
			fis.read(buf);
			fis.close();
			return new String(buf);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			return "-1"; // 文件不存在范围-1，从而方便从下一个读取
		}
	}

	private void write(String chunk_uuid, String chunk) throws IOException {
		// TODO Auto-generated method stub
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(path+chunk_uuid);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("The file in HDFS is broken.");
		}
		fos.write(chunk.getBytes());
		fos.close();
	}

	public static void main(String[] args) throws IOException {
		ServerSocket server = new ServerSocket(12346);
		while (true) {
			// transfer location change Single User or Multi User
			DataNodeServer1 ser = new DataNodeServer1(server.accept());
			ser.start();
		}
	}
}