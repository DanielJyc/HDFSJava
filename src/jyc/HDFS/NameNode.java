package jyc.HDFS;
import java.util.UUID;
import java.io.File;

public class NameNode {

	public NameNode(int chunkloc) {
		super();
		// TODO Auto-generated constructor stub
		String dir_name = Integer.toString(chunkloc); 
		File dir = new File(dir_name);
		if(dir.exists()){
			System.out.println("Ä¿Â¼" + dir_name + "´æÔÚ");
		}
		dir.mkdir();
		
	}

}
