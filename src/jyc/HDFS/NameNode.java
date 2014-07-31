package jyc.HDFS;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.io.File;
import java.util.UUID;


public class NameNode {
	int  num_datanodes = 3;  //num of datanode
	int chunksize = 10;  //size of chunk
	Map filetable = new HashMap(); //��� filename<-->chunkuuids
	Map chunktable = new HashMap(); //chunkuuid<-->chunkloc
	Map datanodes = new HashMap(); //chunkloc <--> datanode	

	public NameNode() {
		// TODO Auto-generated constructor stub
		init_datanodes();  //��ʼ��DataNode(3��)
	}
	
	/*��ʼ����loc<-->server*/
	public void init_datanodes(){
		int i;
		for(i=1; i<=num_datanodes; i++){
			DataNode nd = new DataNode(i);
			datanodes.put(i, nd);
		}
	}
	
	/*���ӳ�䣺filetable��chunktable*/
	public List alloc(String filename, int num_chunks){
		int chunkloc = 1;
		String chunk_uuid;
		List chunku_uids = new ArrayList();
		for (int i = 0; i < num_chunks; i++) {
			chunk_uuid = UUID.randomUUID().toString();
			chunku_uids.add(chunk_uuid);
			chunktable.put(chunk_uuid, chunkloc);
			chunkloc = chunkloc % num_datanodes + 1;			
		}
		filetable.put(filename, chunku_uids);		
		//test
		Iterator it = (Iterator) filetable.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry me = (Entry) it.next();
			System.out.println(me.getKey()+"/"+me.getValue());
		}
		
		Iterator it2 = chunktable.entrySet().iterator();
		while(it2.hasNext()){
			Map.Entry me2 = (Entry) it2.next();
			System.out.println(me2.getKey()+"/"+me2.getValue());
		}
		return chunku_uids;
	}
	
	/*ɾ��NameNode����Ϣ*/
	public void delete(String filename) {
		List chunk_uuids = (List) filetable.get(filename);
		for (Object chunk_uuid : chunk_uuids) {
			chunktable.remove(chunk_uuid);
		filetable.remove(filename);	
		}
	}
	
	/*����ļ��Ƿ���ڣ����ڷ���true�����򷵻�false*/
	public boolean exits(String filename) {
		Iterator it = filetable.keySet().iterator();
		while (it.hasNext()) {
			Object me = (Object) it.next();
//			System.out.println(me);
			if(filename == me){
				return true;
			}
		}
		return true;
	}
}

