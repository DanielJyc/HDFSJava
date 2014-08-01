package jyc.HDFS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jyc.HDFS.DataNode;
import jyc.HDFS.NameNode;
/**
 * 功能：通过调用Client中的方法，对HDFS进行操作：对DataNode和NameNode进行封装。通过调节NameNode和DataNode实现文件的操作。
 * @author DanielJyc
 *
 */
public class Client {
	NameNode namenode;
	public Client(NameNode nn) {  
		this.namenode = nn;
	}
	/**
	 * 功能：写入文件。通过调用DataNode和NameNode的方法，在DataNode写入实际数据；在NameNode写入元数据。
	 * @param filename 文件名
	 * @param data 文件内的数据
	 * @throws IOException 
	 * @throws ClassNotFoundException
	 */
	public void write(String filename, String data) throws IOException, ClassNotFoundException {
		List chunks = new ArrayList();  //存放data分出来的num_chunks份数据
		int chunkloc = 1;
		int num_chunks = get_num_chunks(data);
		for (int i = 0; i < num_chunks; i++) { //将data按照每个chunk大小为chunksize（10byte）进行拆分
			int low = i*namenode.chunksize;
			int high = (i+1)*namenode.chunksize > data.length()? data.length():(i+1)*namenode.chunksize;
			chunks.add(data.subSequence(low, high) );			
		}
		//为文件分配空间，更新元数据
		List chunk_uuids = namenode.alloc(filename, num_chunks);
		for (int i = 0; i < chunk_uuids.size(); i++) {
			chunkloc = i % namenode.num_datanodes + 1;
			((DataNode) namenode.datanodes.get(chunkloc)).write(chunk_uuids.get(i).toString(), chunks.get(i).toString());
			//备份第二份
			chunkloc = chunkloc % namenode.num_datanodes +1;
			((DataNode) namenode.datanodes.get(chunkloc)).write(chunk_uuids.get(i).toString(), chunks.get(i).toString());
		}
	}
	
	/**
	 * 功能：从HDFS读取文件。通过调用DataNode和NameNode的方法，从DataNode读取实际数据；从NameNode读取元数据。
	 * @param filename
	 * @return 返回文件内容data（String）。如果文件不存在，或者DataNode被kill，返回"-1"。
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public String read(String filename) throws IOException, ClassNotFoundException {
		if (true == namenode.exits(filename)) {
			String data = new String();
			List chunk_uuids = (List) namenode.filetable.get(filename);
			for (Object chunk_uuid : chunk_uuids) {
				int chunkloc = (Integer) namenode.chunktable.get(chunk_uuid);
				String data_temp = ((DataNode)namenode.datanodes.get(chunkloc) ).read(chunk_uuid.toString());
				if (data_temp.equals("-1")) { //读取当前DataNode上的chunk不存在或者DataNode被kill（即：某一个DataNode被损坏。注：两个损坏时，可以读取数据，但是不完整（用-1代替））
					System.out.println("Current chunk is broken."); 
					data_temp = ((DataNode)namenode.datanodes.get(chunkloc%namenode.num_datanodes + 1) ).read(chunk_uuid.toString());//读取下一个DataNode的chunk
				}
				data = data + data_temp;
			}
			return data;
		}
		else{
			System.out.println("File:"+filename+ " not exits in HDFS.");
			return "-1";
		}
	}
	/**
	 * 删除文件filename
	 * @param String类型filename。
	 * @return 删除成功，返回true；否则返回false。
	 * @throws ClassNotFoundException
	 * @throws IOException
	 */
	public boolean delete(String filename) throws ClassNotFoundException, IOException {
		if (true ==  namenode.exits(filename)) {
			List chunk_uuids = (List) namenode.filetable.get(filename);
			for (Object chunk_uuid : chunk_uuids) {
				int chunkloc = (Integer) namenode.chunktable.get(chunk_uuid);
				((DataNode)namenode.datanodes.get(chunkloc)).delete(chunk_uuid.toString()); //#物理删除:第一份
				((DataNode)namenode.datanodes.get(chunkloc%namenode.num_datanodes + 1)).delete(chunk_uuid.toString());//#物理删除：第二份
			}
			namenode.delete(filename); //逻辑删除：在元数据删除信息
			return true;
		}
		else{
			return false;
		}
	}
	
	/**
	 * 列出HDFS上面的所有文件名
	 */
	public void list_files() {
		System.out.println("Files:");
		Iterator it = (Iterator) namenode.filetable.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry me = (Entry) it.next();
			System.out.println(me.getKey());	//+":"+me.getValue());
		}
	}
	
	/**
	 * 获取filename中的数据data的chunk数量
	 * @param data：filename中的数据
	 * @return filename中的数据data的chunk数量
	 */
	public int get_num_chunks(String data) {
		return (int) Math.ceil((data.length()*1.0) / namenode.chunksize);
	}
}
