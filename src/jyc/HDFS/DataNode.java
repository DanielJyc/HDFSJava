package jyc.HDFS;
import java.io.*;

public class DataNode {
	public static String readFile(String uuid)throws IOException
	{
		FileInputStream fis = new FileInputStream(uuid);
		byte[] buf = new byte[fis.available()];//����һ���ոպõĻ�������
		fis.read(buf);
		fis.close();		
		return new String(buf);

	}
	
	public static void writeFile(String uuid, String data)throws IOException
	{
		FileOutputStream fos = new FileOutputStream(uuid);
		fos.write("abcde".getBytes());
		fos.close();
	}
}
