package jyc.HDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;
/**
 * 实现功能：通过命令对HDFS操作。共有5个命令：
 * 1. upload: 上传当前目录下的文件到HDFS。操作：upload-->filename
 * 2.download：从HDFS下载文件。操作：download-->filename
 * 3.delete:删除HDFS的文件。操作：delete-->filename
 * 4.ls：列出HDFS所有文件。
 * 5.exits：退出
 * @author DanielJyc
 *
 */

public class Command {
	Client client;
	public Command(Client c) {
		this.client = c;
	}
	
	/**
	 * 利用while循环实现命令行的功能。输入exits，退出。
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public void command_line() throws IOException, ClassNotFoundException {
		while (true) {
			System.out.println("Please input your cmd(input help for info):");
			Scanner sc = new Scanner(System.in);
			String cmd = sc.nextLine();
			switch (cmd) {  //5个命令
			case "upload":  
				upload_cmd();
				continue;
			case "download":
				download_cmd();
				continue;
			case "delete":
				delete_cmd();
				continue;
			case "ls":
				client.list_files();
				continue;
			case "help":
				System.out.println("There are 5 command: upload, download, delete, ls(all files), exits(exit the command)");
				continue;
			case "exits":				
				System.exit(0);				
			default:
				System.out.println("Wrong command.");
				continue;
			}
		}
	}

	/**
	 * 删除命令
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	private void delete_cmd() throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		Scanner sc = new Scanner(System.in);
		String filename = sc.nextLine();
		client.delete(filename);
	}

	/**
	 * 从HDFS下载命令
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void download_cmd() throws IOException, ClassNotFoundException {
		Scanner sc = new Scanner(System.in);
		System.out.println("Input the filename which you want to download in HDFS:");
		String filename = sc.nextLine();  //输入文件名
		String data = client.read(filename);
		System.out.println(data);
		FileOutputStream fos = new FileOutputStream(filename);
		fos.write(data.getBytes());//将文件数据写入本地
		fos.close();
	}

	/**
	 * 上传到HDFS命令
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	private void upload_cmd() throws IOException, ClassNotFoundException {
		Scanner sc = new Scanner(System.in);
		System.out.println("Input the filename which you want to upload in local:");
		String filename = sc.nextLine();
		if(false == client.namenode.exits(filename)){  //HDFS中不存在该文件时，上传。
			FileInputStream fis;
			try {
				fis = new FileInputStream(filename);
				byte[] data = new byte[fis.available()];//定义一个刚刚好的缓冲区。
				fis.read(data);
				fis.close();	
				client.write(filename, new String(data));
			} catch (FileNotFoundException e) {
				System.out.println("No such file in local. ");  //文件不存在范围-1，从而方便从下一个读取
			}	
		}
		else{
			System.out.println("File:"+filename+" has exits in HDFS.");
		}
	}
	
}
