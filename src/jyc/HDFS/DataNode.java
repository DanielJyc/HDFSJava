package jyc.HDFS;

import java.io.*;
import java.net.*;
/**
 * ���ܣ���DataNode�������ӣ��������ݴ�����ͨ��
 * @author DanielJyc
 *
 */
public class DataNode {
	private String dir_name;
	private int port;
	

	/**
	 * ��ʼ������ͬ��chunkloc��Ӧ��ͬ��DataNode��ͨ���޸�portʵ�֡�
	 * �������ͨ�������Ų�ͬ��IP�Ͷ˿ںţ��Ӷ�ʵ��������������紫�䡣
	 * @param chunkloc����ͬ��chunkloc��Ӧ��ͬ��DataNodeServer��Ĭ�Ϲ�3����
	 */
	public DataNode(int chunkloc) {  
		super();
		this.port = chunkloc + 12345;
	}
	
	/**
	 * ��chunkд��chunk_uuid(�ļ���)
	 * @param chunk_uuid ��������chunk����ΪString��
	 */
	public void write(String chunk_uuid, String chunk)  {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port); // �򱾻���port�˿ڷ����ͻ�����
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("No server");			
		}
		ObjectOutputStream out = null;
		try {
			out = new ObjectOutputStream(
					client.getOutputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			ObjectInputStream in = new ObjectInputStream(client.getInputStream());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		String[] s = new String[] { "write", chunk_uuid, chunk};
		try {
			out.writeObject(s);
			out.flush(); // ˢ���������ʹServer�����յ����ַ���
			client.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}  // ����
	}
	
	/**
	 * ��chunk_uuid(�ļ���)��ȡchunk
	 * @param String ���͵�chunk_uuid
	 * @return ���ش�chunk_uuid��ȡ�������ݣ�String���͡�
	 */
	public String read(String chunk_uuid) {
		Socket client = null;
		try {
			client = new Socket(InetAddress.getLocalHost(), port);// �򱾻���port�˿ڷ����ͻ�����
		}catch (IOException e) {
			// TODO Auto-generated catch block
			return "-1";
		} 
		ObjectOutputStream out = null;
		try {out = new ObjectOutputStream(client.getOutputStream());} 
			catch (IOException e) {e.printStackTrace();}
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(client.getInputStream());
		} catch (IOException e) {
			e.printStackTrace();
		}		
		String[] s = new String[] { "read", chunk_uuid};
		try {// ����
			out.writeObject(s);
			out.flush(); // ˢ���������ʹServer�����յ����ַ���		
		} catch (IOException e) {
			e.printStackTrace();
		}  
		String[] arr = null;
		try {//����
			arr = (String[]) in.readObject();
			client.close();

		} catch (ClassNotFoundException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return arr[0].toString();
	}

	/**
	 * ɾ��chunk_uuid(�ļ���)
	 * @param  String ���͵�chunk_uuid
	 */
	public void delete(String chunk_uuid)   {
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
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
}
