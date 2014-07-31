package jyc.HDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class DataNodeServer {
	public static void main(String[] args) throws IOException {
		ServerSocket server = new ServerSocket(5678);  //创建一个ServerSocket在端口4700监听客户请求
		while (true) {
			Socket client = server.accept(); //使用accept()阻塞等待客户请求，有客户
			BufferedReader in = new BufferedReader(new InputStreamReader(
					client.getInputStream())); //由Socket对象得到输入流，并构造相应的BufferedReader对象
			PrintWriter out = new PrintWriter(client.getOutputStream()); //由Socket对象得到输出流，并构造PrintWriter对象
			while (true) {
				String str = in.readLine(); //从Client读入一字符串，并打印到标准输出上
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
