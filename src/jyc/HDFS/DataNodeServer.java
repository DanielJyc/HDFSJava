package jyc.HDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataNodeServer {
	public static void main(String[] args) throws IOException {
		ServerSocket server = new ServerSocket(5678);  //����һ��ServerSocket�ڶ˿�4700�����ͻ�����
		while (true) {
			Socket client = server.accept(); //ʹ��accept()�����ȴ��ͻ������пͻ�
			BufferedReader in = new BufferedReader(new InputStreamReader(
					client.getInputStream())); //��Socket����õ�����������������Ӧ��BufferedReader����
			PrintWriter out = new PrintWriter(client.getOutputStream()); //��Socket����õ��������������PrintWriter����
			while (true) {
				String str = in.readLine(); //��Client����һ�ַ���������ӡ����׼�����
				System.out.println(str);
				out.println("has receive....");
				out.flush();
				if (str.equals("end"))
					break;
			}
			client.close();
		}
	}
}
