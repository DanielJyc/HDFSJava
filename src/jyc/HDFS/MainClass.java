package jyc.HDFS;
import java.io.IOException;
import java.util.*;
import java.util.UUID;

import jyc.HDFS.DataNode;
public class MainClass {
	private static final Integer Integer = null;

	public static void main(String[] args) throws IOException{
		DataNode tt = new DataNode();
		UUID uuid = UUID.randomUUID();
		System.out.println(uuid);
		tt.writeFile(uuid.toString(), "test");
		tt.readFile(uuid.toString());
		
	}

}
