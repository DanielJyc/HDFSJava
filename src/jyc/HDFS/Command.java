package jyc.HDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;

public class Command {
	Client client;
	public Command(Client c) {
		// TODO Auto-generated constructor stub
		client = c;
	}
	
	public void command_line() throws IOException, ClassNotFoundException {
		while (true) {
			Scanner sc = new Scanner(System.in);
			String cmd = sc.nextLine();
			switch (cmd) {  //5������
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
			case "exits":				
				System.exit(0);;
			default:
				System.out.println("Wrong command.");
				continue;
			}
		}
	}

	private void delete_cmd() throws ClassNotFoundException, IOException {
		// TODO Auto-generated method stub
		Scanner sc = new Scanner(System.in);
		String filename = sc.nextLine();
		client.delete(filename);
	}

	private void download_cmd() throws IOException, ClassNotFoundException {
		Scanner sc = new Scanner(System.in);
		System.out.println("Input the filename which you want to download in HDFS:");
		String filename = sc.nextLine();  //�����ļ���
		String data = client.read(filename);
		System.out.println(data);
		FileOutputStream fos = new FileOutputStream(filename);
		fos.write(data.getBytes());//���ļ�����д�뱾��
		fos.close();
	}

	private void upload_cmd() throws IOException, ClassNotFoundException {
		Scanner sc = new Scanner(System.in);
		System.out.println("Input the filename which you want to upload in local:");
		String filename = sc.nextLine();
		if(false == client.namenode.exits(filename)){  //HDFS�в����ڸ��ļ�ʱ���ϴ���
			FileInputStream fis;
			try {
				fis = new FileInputStream(filename);
				byte[] data = new byte[fis.available()];//����һ���ոպõĻ�������
				fis.read(data);
				fis.close();	
				client.write(filename, new String(data));
			} catch (FileNotFoundException e) {
				System.out.println("No such file in local. ");  //�ļ������ڷ�Χ-1���Ӷ��������һ����ȡ
			}	
		}
		else{
			System.out.println("File:"+filename+" has exits in HDFS.");
		}
	}
	
}
