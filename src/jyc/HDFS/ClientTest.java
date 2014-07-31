package jyc.HDFS;

import java.net.*;
import java.io.*;

public class ClientTest {
	static Socket server;

	public static void main(String[] args) throws Exception {
		server = new Socket(InetAddress.getLocalHost(), 5678); //向本机的5678端口发出客户请求
		BufferedReader in = new BufferedReader(new InputStreamReader(
				server.getInputStream())); //由Socket对象得到输入流，并构造相应的BufferedReader对象
 		PrintWriter out = new PrintWriter(server.getOutputStream()); //由Socket对象得到输出流，并构造PrintWriter对象
		BufferedReader wt = new BufferedReader(new InputStreamReader(System.in)); //由系统标准输入设备构造BufferedReader对象

		while (true) {
			String str = wt.readLine();
			out.println(str); //将从系统标准输入读入的字符串输出到Server
			out.flush(); //刷新输出流，使Server马上收到该字符串
			if (str.equals("end")) {
				break;
			}
			System.out.println(in.readLine());
		}
		server.close();
	}
}