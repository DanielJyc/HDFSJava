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
			System.out.println("目录" + dir_name + "存在");
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
			byte[] buf = new byte[fis.available()];//定义一个刚刚好的缓冲区。
			fis.read(buf);
			fis.close();		
			return new String(buf);			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			return "-1";  //文件不存在范围-1，从而方便从下一个读取
		}

	}
	
	/*删除成功返回true 否则返回false（文件不存在）*/
	public boolean delete(String uuid)
	{
		File file = new File(dir_name+File.separator+uuid);
		if(!file.exists()){		//不存在，返回false
			return false;
		}
		else{
			return file.delete();			
		}
	}
}
