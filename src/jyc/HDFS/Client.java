package jyc.HDFS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jyc.HDFS.DataNode;
import jyc.HDFS.NameNode;

public class Client {
	NameNode namenode;
	public Client(NameNode nn) {
		// TODO Auto-generated constructor stub
		namenode = nn;
	}
	
	public void write(String filename, String data) throws IOException {
		List chunks = new ArrayList();  //���data�ֳ�����num_chunks������
		int chunkloc = 1;
		int num_chunks = get_num_chunks(data);
		for (int i = 0; i < num_chunks; i++) { //��data����ÿ��chunk��СΪchunksize��10byte�����в��
			int low = i*namenode.chunksize;
			int high = (i+1)*namenode.chunksize > data.length()? data.length():(i+1)*namenode.chunksize;
			chunks.add(data.subSequence(low, high) );			
		}
		//test
//		for (Object chunk: chunks) {
//			System.out.println(chunk);
//		}
		//Ϊ�ļ�����ռ䣬����Ԫ����
		List chunk_uuids = namenode.alloc(filename, num_chunks);
		for (int i = 0; i < chunk_uuids.size(); i++) {
			chunkloc = i % namenode.num_datanodes + 1;
			((DataNode) namenode.datanodes.get(chunkloc)).write(chunk_uuids.get(i).toString(), chunks.get(i).toString());
			//���ݵڶ���
			chunkloc = chunkloc % namenode.num_datanodes +1;
			((DataNode) namenode.datanodes.get(chunkloc)).write(chunk_uuids.get(i).toString(), chunks.get(i).toString());
		}
	}
	
	public String read(String filename) throws IOException {
		if (true == namenode.exits(filename)) {
			String data = new String();
			List chunk_uuids = (List) namenode.filetable.get(filename);
			for (Object chunk_uuid : chunk_uuids) {
				int chunkloc = (Integer) namenode.chunktable.get(chunk_uuid);
				String data_temp = ((DataNode)namenode.datanodes.get(chunkloc) ).read(chunk_uuid.toString());
				if ("-1" == data_temp) { //��ȡ��ǰDataNode�ϵ�chunk�����ڣ�����ĳһ��DataNode���𻵡�ע��������ʱ�����Զ�ȡ���ݣ����ǲ���������-1���棩��
					data_temp = ((DataNode)namenode.datanodes.get(chunkloc%namenode.num_datanodes + 1) ).read(chunk_uuid.toString());//��ȡ��һ��DataNode��chunk
					System.out.println("Current chunk is broken."); 
				}
				data = data + data_temp;
			}
			return data;
		}
		else{
			System.out.println("File:"+filename+ "not exits.");
			return "-1";
		}
	}
	
	public boolean delete(String filename) {
		if (true ==  namenode.exits(filename)) {
			List chunk_uuids = (List) namenode.filetable.get(filename);
			for (Object chunk_uuid : chunk_uuids) {
				int chunkloc = (Integer) namenode.chunktable.get(chunk_uuid);
				((DataNode)namenode.datanodes.get(chunkloc)).delete(chunk_uuid.toString()); //#����ɾ��:��һ��
				((DataNode)namenode.datanodes.get(chunkloc%namenode.num_datanodes + 1)).delete(chunk_uuid.toString());//#����ɾ�����ڶ���
			}
			namenode.delete(filename); //�߼�ɾ������Ԫ����ɾ����Ϣ
			return true;
		}
		else{
			return false;
		}		
	}
	
	/*�г������ļ���*/
	public void list_files() {
		System.out.println("Files:");
		Iterator it = (Iterator) namenode.filetable.entrySet().iterator();
		while(it.hasNext()){
			Map.Entry me = (Entry) it.next();
			System.out.println(me.getKey());	//+":"+me.getValue());
		}
	}
	
	/*��ȡchunk����*/
	public int get_num_chunks(String data) {
		return (int) Math.ceil((data.length()*1.0) / namenode.chunksize);
	}
}
