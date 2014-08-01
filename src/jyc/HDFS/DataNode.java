package jyc.HDFS;

import java.io.*;
import java.net.*;

public class DataNode {
	private String dir_name;
	private int port;
	

	/*��ʼ������ͬ��chunkloc��Ӧ��ͬ��DataNode��ͨ���޸�portʵ�֡�
	 * �������ͨ�������Ų�ͬ��IP�Ͷ˿ںţ��Ӷ�ʵ��������������紫�䡣*/
	public DataNode(int chunkloc) {  
		super();
		this.port = chunkloc + 12345;
	}
	
	/*��chunkд��chunk_uuid(�ļ���)*/
	public void write(String chunk_uuid, String chunk) throws IOException, ClassNotFoundException {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port); // �򱾻���port�˿ڷ����ͻ�����
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("No server");			
		}
		ObjectOutputStream out = new ObjectOutputStream(
				client.getOutputStream());
		ObjectInputStream in = new ObjectInputStream(client.getInputStream());		
		String[] s = new String[] { "write", chunk_uuid, chunk};
		out.writeObject(s);  // ����
		out.flush(); // ˢ���������ʹServer�����յ����ַ���
		client.close();
	}
	
	/*��chunk_uuid(�ļ���)��ȡchunk*/
	public String read(String chunk_uuid) throws IOException, ClassNotFoundException  {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port);// �򱾻���port�˿ڷ����ͻ�����
		}catch (IOException e) {
			// TODO Auto-generated catch block
			return "-1";
		} 
		ObjectOutputStream out = new ObjectOutputStream(
				client.getOutputStream());
		ObjectInputStream in = new ObjectInputStream(client.getInputStream());		
		String[] s = new String[] { "read", chunk_uuid};
		out.writeObject(s);  // ����
		out.flush(); // ˢ���������ʹServer�����յ����ַ���		
		String [] arr = (String[]) in.readObject(); //����
		client.close();
		return arr[0].toString();
	}

	/* ɾ��chunk_uuid(�ļ���) */
	public void delete(String chunk_uuid) throws IOException, ClassNotFoundException {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port);// �򱾻���port�˿ڷ����ͻ�����
			ObjectOutputStream out = new ObjectOutputStream(
					client.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(client.getInputStream());		
			String[] s = new String[] { "delete", chunk_uuid};
			out.writeObject(s);// ����
			out.flush(); // ˢ���������ʹServer�����յ����ַ���		
			String [] arr = (String[]) in.readObject();//����
			client.close();
			System.out.println(arr[0]);
		}catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println( "Server not exits.");
		} 
	}
}
