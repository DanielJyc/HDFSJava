package jyc.HDFS;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import jyc.HDFS.DataNode;
public class MainClass {
	private static final Integer Integer = null;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		//Test all.
		NameNode nn = new NameNode();
		Client c = new Client(nn);
		Command cmd =new Command(c);
		cmd.command_line();
		
		//Test net-DataNode
//		DataNode dn = new DataNode(2);
//		dn.write("jyc", "0123456789abceefghijklmnopqrstuvwxyz");
//		System.out.println(dn.read("jyc"));
//		System.out.println(dn.read("jyc2"));
//		Thread.currentThread().sleep(3000);
//		dn.delete("jyc");
		
		
//		System.out.println(System.getProperty("user.dir"));//user.dir指定了当前的路径
//		
//		Scanner sc = new Scanner(System.in);
//		System.out.println("Input the filename which you want to download in HDFS:");
//		String filename = sc.nextLine();  //输入文件名
//		FileInputStream fis;
//		fis = new FileInputStream("jyc");
//		byte[] buf = new byte[fis.available()];//定义一个刚刚好的缓冲区。
//		fis.read(buf);		
//		String data = new String(buf);
//		System.out.println(data);
//		
		
		//Test NameNode
//		NameNode nn = new NameNode();
//		nn.test();
//		nn.alloc("test", 5);
//		nn.delete("test");
//		System.out.println(nn.exits("test"));
		
		//test cmd
		
//		Scanner sc = new Scanner(System.in);
//		String cmd = sc.nextLine();
//		System.out.println(cmd);
		
		//test Client
//		Client c = new Client(nn);
//		System.out.println(c.get_num_chunks("01234567890123456789012345678912"));
//		c.write("jyc", "01234567890123456789012345678912");
//		System.out.println(c.read("jyc"));
//		Thread.currentThread().sleep(3000);
//		System.out.println(c.delete("jyc"));
//		c.list_files();
//		String s = "01234567890123456789012345678912";
//		System.out.println(s.subSequence(0, 32));
//		s.subSequence(0, 3);
		//Test DataNode.
	/*	DataNode dn = new DataNode(1);
		UUID uuid = UUID.randomUUID();
		System.out.println(uuid);
		dn.write(uuid.toString(), "test");
		System.out.println(dn.read(uuid.toString()));
		
		DataNode dns = new DataNode(2);
		UUID uuids = UUID.randomUUID();
		System.out.println(uuids);
		dns.write(uuids.toString(), "test");
		System.out.println(dns.read(uuids.toString()));

		Thread.currentThread().sleep(3000);
		System.out.println(dn.delete(uuid.toString()));
		System.out.println(dns.delete(uuids.toString()));  */
	}
}
