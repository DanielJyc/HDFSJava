package jyc.HDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class MainClass {
	public static void main(String[] args) {
		NameNode nn = new NameNode();
		Client c = new Client(nn);
		Command cmd =new Command(c);
		cmd.command_line();
		
	}
}
