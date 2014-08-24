package jyc.HDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
/**
 * ʵ�ֹ��ܣ���������ڡ����ļ�namenode.out��ȡ�ϴ��˳�ʱ�����NameNode��Ϣ��Ȼ��ʼ����
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
	 * ���ܣ������к��ļ�namenode.out��ȡ�ϴ��˳�ʱ�����NameNode��Ϣ
	 * @return ��ȡ����NameNode
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
