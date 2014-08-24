package jyc.HDFS;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
/**
 * ʵ��NameNode����Ҫ����ʵ��Ԫ���ݣ���ȡ�ļ�������Ϣ
 * @author DanielJyc
 *
 */
public class NameNode {
	int num_datanodes = 3; // num of datanode
	int chunksize = 10; // size of chunk
	Map<String, List<String>> filetable = new HashMap<String, List<String>>(); // ��� filename<-->chunkuuids
	Map<String, Integer> chunktable = new HashMap<String, Integer>(); // chunkuuid<-->chunkloc
	Map<Integer, DataNode> datanodes = new HashMap<Integer, DataNode>(); // chunkloc <--> datanode

	public NameNode() {
		init_datanodes(); // ��ʼ��DataNode(3��)
	}

	/**
	 * ��ʼ��NameNode��NameNode����
	 */
	public void init_datanodes() {
		int i;
		for (i = 1; i <= num_datanodes; i++) {
			DataNode nd = new DataNode(i);
			datanodes.put(i, nd);
		}
	}

	/**
	 *  ���ӳ�䣺filetable��chunktable 
	 *  @param �ļ���filename, chunks����num_chunks
	 *  @return List���͵�chunk_uuids�������ļ�filename��Ӧ�����е�uuid
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
		// ��ӳ�䡢����Ľ����ӡ
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
	 *  �߼�ɾ����ɾ��NameNode����Ϣ
	 *  @param filename
	 *  @return ��
	 */
	public void delete(String filename) {
		List<String> chunk_uuids = filetable.get(filename);
		for (Object chunk_uuid : chunk_uuids) {
			chunktable.remove(chunk_uuid);
			filetable.remove(filename);
		}
	}

	/**
	 *  ����ļ��Ƿ����
	 *   @param filename
	 *   @return ���ڷ���true�����򷵻�false
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
