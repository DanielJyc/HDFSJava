package jyc.HDFS;
import java.io.*;

public class DataNode {
	public static void readFile(String uuid)throws IOException
	{
		FileInputStream fis = new FileInputStream(uuid);
		byte[] buf = new byte[fis.available()];//����һ���ոպõĻ�������
		fis.read(buf);
		System.out.println(new String(buf));
		fis.close();
	}
	
	public static void writeFile(String uuid, String data)throws IOException
	{
		FileOutputStream fos = new FileOutputStream(uuid);
		fos.write("abcde".getBytes());
		fos.close();
	}
}
