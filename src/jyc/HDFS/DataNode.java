package jyc.HDFS;
import java.io.*;

public class DataNode {
		private String dir_name;

		public DataNode(int chunkloc) {
		super();
		// TODO Auto-generated constructor stub
		dir_name = "D:"+File.separator+"DataNode"+File.separator+Integer.toString(chunkloc); 
		File dir = new File(dir_name);
		if(dir.exists()){
			System.out.println("Ŀ¼" + dir_name + "����");
		}
//		System.out.println(dir);
		dir.mkdirs();		
	}

	public void write(String uuid, String data)throws IOException
	{
		FileOutputStream fos = new FileOutputStream(dir_name+File.separator+uuid);
		fos.write(data.getBytes());
		fos.close();
	}		
	
	public String read(String uuid) throws IOException
	{
		FileInputStream fis;
		try {
			fis = new FileInputStream(dir_name+File.separator+uuid);
			byte[] buf = new byte[fis.available()];//����һ���ոպõĻ�������
			fis.read(buf);
			fis.close();		
			return new String(buf);			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			return "-1";  //�ļ������ڷ�Χ-1���Ӷ��������һ����ȡ
		}

	}
	
	/*ɾ���ɹ�����true ���򷵻�false���ļ������ڣ�*/
	public boolean delete(String uuid)
	{
		File file = new File(dir_name+File.separator+uuid);
		if(!file.exists()){		//�����ڣ�����false
			return false;
		}
		else{
			return file.delete();			
		}
	}
}
