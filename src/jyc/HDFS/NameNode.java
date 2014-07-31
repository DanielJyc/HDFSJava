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
	Map filetable = new HashMap(); //存放 filename<-->chunkuuids
	Map chunktable = new HashMap(); //chunkuuid<-->chunkloc
	Map datanodes = new HashMap(); //chunkloc <--> datanode	

	public NameNode() {
		// TODO Auto-generated constructor stub
		init_datanodes();  //初始化DataNode(3个)
	}
	
	/*初始化：loc<-->server*/
	public void init_datanodes(){
		int i;
		for(i=1; i<=num_datanodes; i++){
			DataNode nd = new DataNode(i);
			datanodes.put(i, nd);
		}
	}
	
	/*完成映射：filetable和chunktable*/
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
	
	/*删除NameNode中信息*/
	public void delete(String filename) {
		List chunk_uuids = (List) filetable.get(filename);
		for (Object chunk_uuid : chunk_uuids) {
			chunktable.remove(chunk_uuid);
		filetable.remove(filename);	
		}
	}
	
	/*检测文件是否存在：存在返回true；否则返回false*/
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

