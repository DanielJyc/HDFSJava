package jyc.HDFS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
/**
 * 实现NameNode，主要作用实现元数据，获取文件保存信息
 * @author DanielJyc
 *
 */
public class NameNode {
	int num_datanodes = 3; // num of datanode
	int chunksize = 10; // size of chunk
	Map<String, List<String>> filetable = new HashMap<String, List<String>>(); // 存放 filename<-->chunkuuids
	Map<String, Integer> chunktable = new HashMap<String, Integer>(); // chunkuuid<-->chunkloc
	Map<Integer, DataNode> datanodes = new HashMap<Integer, DataNode>(); // chunkloc <--> datanode

	public NameNode() {
		init_datanodes(); // 初始化DataNode(3个)
	}

	/**
	 * 初始化NameNode：NameNode个数
	 */
	public void init_datanodes() {
		int i;
		for (i = 1; i <= num_datanodes; i++) {
			DataNode nd = new DataNode(i);
			datanodes.put(i, nd);
		}
	}

	/**
	 *  完成映射：filetable和chunktable 
	 *  @param 文件名filename, chunks个数num_chunks
	 *  @return List类型的chunk_uuids：保存文件filename对应的所有的uuid
	 */
	public List<String> alloc(String filename, int num_chunks) {
		int chunkloc = 1;
		String chunk_uuid;
		List<String> chunk_uuids = new ArrayList<String>();
		for (int i = 0; i < num_chunks; i++) {
			chunk_uuid = UUID.randomUUID().toString();
			chunk_uuids.add(chunk_uuid);
			chunktable.put(chunk_uuid, chunkloc);
			chunkloc = chunkloc % num_datanodes + 1;
		}
		filetable.put(filename, chunk_uuids);
		// 将映射、分配的结果打印
		Iterator it = (Iterator) filetable.entrySet().iterator();
		while (it.hasNext()) {
			@SuppressWarnings("unchecked")
			Map.Entry<String, List<String>>  me = (Entry<String, List<String>> ) it.next();
			System.out.println(me.getKey() + "/" + me.getValue());
		}
		Iterator it2 = chunktable.entrySet().iterator();
		while (it2.hasNext()) {
			@SuppressWarnings("unchecked")
			Map.Entry<String, List<String>>  me2 = (Entry<String, List<String>> ) it2.next();
			System.out.println(me2.getKey() + "/" + me2.getValue());
		}
		return chunk_uuids;
	}

	/**
	 *  逻辑删除：删除NameNode中信息
	 *  @param filename
	 *  @return 无
	 */
	public void delete(String filename) {
		List<String> chunk_uuids = filetable.get(filename);
		for (Object chunk_uuid : chunk_uuids) {
			chunktable.remove(chunk_uuid);
			filetable.remove(filename);
		}
	}

	/**
	 *  检测文件是否存在
	 *   @param filename
	 *   @return 存在返回true；否则返回false
	 */
	public boolean exits(String filename) {
		Iterator<String> it = filetable.keySet().iterator();
		while (it.hasNext()) {
			Object me = it.next();
			if (filename.equals(me)) {
				return true;
			}
		}
		return false;
	}

}
