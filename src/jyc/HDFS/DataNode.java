package jyc.HDFS;

import java.io.*;
import java.net.*;
/**
 * 功能：与DataNode建立连接，进行数据传输与通信
 * @author DanielJyc
 *
 */
public class DataNode {
	private String dir_name;
	private int port;
	

	/**
	 * 初始化：不同的chunkloc对应不同的DataNode；通过修改port实现。
	 * 后面可以通过数组存放不同的IP和端口号，从而实现真正意义的网络传输。
	 * @param chunkloc：不同的chunkloc对应不同的DataNodeServer，默认共3个。
	 */
	public DataNode(int chunkloc) {  
		super();
		this.port = chunkloc + 12345;
	}
	
	/**
	 * 将chunk写入chunk_uuid(文件名)
	 * @param chunk_uuid 及其内容chunk。均为String。
	 */
	public void write(String chunk_uuid, String chunk) throws IOException, ClassNotFoundException {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port); // 向本机的port端口发出客户请求
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("No server");			
		}
		ObjectOutputStream out = new ObjectOutputStream(
				client.getOutputStream());
		ObjectInputStream in = new ObjectInputStream(client.getInputStream());		
		String[] s = new String[] { "write", chunk_uuid, chunk};
		out.writeObject(s);  // 发送
		out.flush(); // 刷新输出流，使Server马上收到该字符串
		client.close();
	}
	
	/**
	 * 从chunk_uuid(文件名)读取chunk
	 * @param String 类型的chunk_uuid
	 * @return 返回从chunk_uuid读取到的内容：String类型。
	 */
	public String read(String chunk_uuid) throws IOException, ClassNotFoundException  {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port);// 向本机的port端口发出客户请求
		}catch (IOException e) {
			// TODO Auto-generated catch block
			return "-1";
		} 
		ObjectOutputStream out = new ObjectOutputStream(
				client.getOutputStream());
		ObjectInputStream in = new ObjectInputStream(client.getInputStream());		
		String[] s = new String[] { "read", chunk_uuid};
		out.writeObject(s);  // 发送
		out.flush(); // 刷新输出流，使Server马上收到该字符串		
		String [] arr = (String[]) in.readObject(); //接收
		client.close();
		return arr[0].toString();
	}

	/**
	 * 删除chunk_uuid(文件名)
	 * @param  String 类型的chunk_uuid
	 */
	public void delete(String chunk_uuid) throws IOException, ClassNotFoundException {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port);// 向本机的port端口发出客户请求
			ObjectOutputStream out = new ObjectOutputStream(
					client.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(client.getInputStream());		
			String[] s = new String[] { "delete", chunk_uuid};
			out.writeObject(s);// 发送
			out.flush(); // 刷新输出流，使Server马上收到该字符串		
			String [] arr = (String[]) in.readObject();//接收
			client.close();
			System.out.println(arr[0]);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println( "Server not exits.");
		} 
	}
}
