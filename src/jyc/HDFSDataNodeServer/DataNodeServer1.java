package jyc.HDFSDataNodeServer;

import java.net.*;
import java.io.*;

/**
 * ʹ��socket Serverʵ��DataNode��һֱ���ڼ���״̬���Դ�������ݽ��д���
 * ���յ���Object(String [])���ݽ����жϣ�������arr[0]Ϊ��������arr[1]Ϊ�ļ�����arr[2]Ϊchunk�ļ����ݡ�
 * ���У�arr[1]��arr[2]Ϊ��ѡ�
 * ����arr[0]�� ��ͬ��������ͬ��Ӧ��
 * 1.write:��ʱ��arr[1]Ϊ�ļ���(chunk_uuid)��arr[2]Ϊchunk�ļ����ݡ�������д����ļ��С�
 * 2.read����ȡ�ļ���(chunk_uuid)�����ݲ����ء�
 * 3.delete��ɾ���ļ���(chunk_uuid)�����ݲ������Ƿ�ɹ���
 * ע��������������ʱ������ʾ��Wrong command��
 * @author DanielJyc
 *
 */
public class DataNodeServer1 extends Thread {
	private Socket server;
	private String path ="."+ File.separator + "DataNode1" + File.separator;

	/**
	 * ���캯������ʼ����Ϊÿһ��DataNodeServer������ͬ��Ŀ¼�����chunk���ݡ�
	 * @param ser
	 */
	public DataNodeServer1(Socket ser) {
		this.server = ser;
		File dir = new File(path);
		if(dir.exists()){
			System.out.println("Ŀ¼" + path + "����");
		}
		dir.mkdirs();
	}

	/**
	 * ���յ���Object(String [])���ݽ����жϣ�������arr[0]Ϊ��������arr[1]Ϊ�ļ�����arr[2]Ϊchunk�ļ����ݡ�
	 * ���У�arr[1]��arr[2]Ϊ��ѡ�
	 * ����arr[0]�� ��ͬ��������ͬ��Ӧ��
	 * 1.write:��ʱ��arr[1]Ϊ�ļ���(chunk_uuid)��arr[2]Ϊchunk�ļ����ݡ�������д����ļ��С�
	 * 2.read����ȡ�ļ���(chunk_uuid)�����ݲ����ء�
	 * 3.delete��ɾ���ļ���(chunk_uuid)�����ݲ������Ƿ�ɹ���
	 * ע��������������ʱ������ʾ��Wrong command��
	 */
	public void run() {
		try {
			ObjectOutputStream out = new ObjectOutputStream(server.getOutputStream());
			ObjectInputStream in = new ObjectInputStream(server.getInputStream());
			// ����--Mutil User but can't parallel
			String [] arr= (String[]) in.readObject();
			System.out.println(arr[0]+arr[1]);
			switch (arr[0]) {
			case "write":
				System.out.println("Write command.");
				write(arr[1], arr[2]);
				break;
			case "read":				
				System.out.println("Read command.");
				String data = read(arr[1]); //�ӱ��ض�ȡ�ļ�����
				String[] s =new String[]{data};
				out.writeObject(s);  //����
				out.flush();			
				break;
			case "delete":
				System.out.println("Read command.");
				String[] s1 = null;
				if(true == delete(arr[1])){		//�ӱ��ض�ȡ�ļ�����
					s1 =new String[]{"Delete done."};
				}
				else {
					s1 =new String[]{"Filename dose not exits."};
				}
				out.writeObject(s1);  //����
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
			ex.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	/**
	 * delete��ɾ���ļ���(chunk_uuid)�����ݲ������Ƿ�ɹ���
	 * @param chunk_uuid
	 * @return ɾ���ɹ�����true�����򷵻�false
	 */
	private boolean delete(String chunk_uuid) {
		// TODO Auto-generated method stub
		File file = new File( path+chunk_uuid);
		if (!file.exists()) { // �����ڣ�����false
			return false;
		} else {
			return file.delete();
		}
	}

	/**
	 * 2.read����ȡ�ļ���(chunk_uuid)�����ݲ����ء�
	 * @param chunk_uuid
	 * @return �����ļ����ݡ� ��ȡʧ�ܣ����ر�ʶ-1������Client����һ��DataNode��ȡ��
	 */
	private String read(String chunk_uuid)  {
		// TODO Auto-generated method stub
		FileInputStream fis;
		try {
			fis = new FileInputStream(path+chunk_uuid);
			byte[] buf = new byte[fis.available()];// ����һ���ոպõĻ�������
			fis.read(buf);
			fis.close();
			return new String(buf);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			return "-1"; // �ļ������ڷ�Χ-1���Ӷ��������һ����ȡ
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return "-1"; 
		}
	}

	/**
	 * 1.write:��ʱ��arr[1]Ϊ�ļ���(chunk_uuid)��arr[2]Ϊchunk�ļ����ݡ�������д����ļ��С�
	 * @param chunk_uuid  arr[1]Ϊ�ļ���(chunk_uuid)
	 * @param chunk  arr[2]Ϊchunk�ļ�����
	 * IOException ���ļ������ڵ�ʱ�������"The file in HDFS is broken."
	 */
	private void write(String chunk_uuid, String chunk)  {
		// TODO Auto-generated method stub
		FileOutputStream fos = null;
		try {
			fos = new FileOutputStream(path+chunk_uuid);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("The file in HDFS is broken.");
		}
		try {
			fos.write(chunk.getBytes());
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * ��������ڣ�����socket���������ݣ������̡߳�
	 * @param args
	 */
	@SuppressWarnings("resource")
	public static void main(String[] args)  {
		ServerSocket server = null;
		try {
			server = new ServerSocket(12346);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while (true) {
			// transfer location change Single User or Multi User
			DataNodeServer1 ser = null;
			try {
				ser = new DataNodeServer1(server.accept());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			ser.start();
		}
	}
}