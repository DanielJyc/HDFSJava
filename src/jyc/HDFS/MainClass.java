package jyc.HDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
/**
 * 实现功能：主程序入口。从文件namenode.out读取上次退出时保存的NameNode信息；然后开始程序。
 * @author DanielJyc
 *
 */
public class MainClass {
	public static void main(String[] args) {
		NameNode nn = NameNodeDesserial();
		Client c = new Client(nn);
		Command cmd =new Command(c);
		cmd.command_line();
		
	}
	/***
	 * 功能：从序列号文件namenode.out读取上次退出时保存的NameNode信息
	 * @return 读取到的NameNode
	 */
	public static NameNode NameNodeDesserial() {
        File file = new File("namenode.out");
        ObjectInputStream oin;
		NameNode namenode = null;
		try {
			oin = new ObjectInputStream(new FileInputStream(file));
	        namenode = (NameNode) oin.readObject();
	        oin.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return namenode;
	}
}
