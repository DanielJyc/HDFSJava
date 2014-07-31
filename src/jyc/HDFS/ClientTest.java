package jyc.HDFS;

import java.net.*;
import java.io.*;

public class ClientTest {
	static Socket server;

	public static void main(String[] args) throws Exception {
		server = new Socket(InetAddress.getLocalHost(), 5678); //�򱾻���5678�˿ڷ����ͻ�����
		BufferedReader in = new BufferedReader(new InputStreamReader(
				server.getInputStream())); //��Socket����õ�����������������Ӧ��BufferedReader����
 		PrintWriter out = new PrintWriter(server.getOutputStream()); //��Socket����õ��������������PrintWriter����
		BufferedReader wt = new BufferedReader(new InputStreamReader(System.in)); //��ϵͳ��׼�����豸����BufferedReader����

		while (true) {
			String str = wt.readLine();
			out.println(str); //����ϵͳ��׼���������ַ��������Server
			out.flush(); //ˢ���������ʹServer�����յ����ַ���
			if (str.equals("end")) {
				break;
			}
			System.out.println(in.readLine());
		}
		server.close();
	}
}