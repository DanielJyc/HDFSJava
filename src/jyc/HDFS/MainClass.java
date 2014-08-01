package jyc.HDFS;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * ³ÌÐòÈë¿Ú
 * @author DanielJyc
 */
import jyc.HDFS.DataNode;
public class MainClass {
	private static final Integer Integer = null;

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		NameNode nn = new NameNode();
		Client c = new Client(nn);
		Command cmd =new Command(c);
		cmd.command_line();
	}
}
