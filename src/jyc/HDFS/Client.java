package jyc.HDFS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import jyc.HDFS.DataNode;
import jyc.HDFS.NameNode;
/**
 * ���ܣ�ͨ������Client�еķ�������HDFS���в�������DataNode��NameNode���з�װ��ͨ������NameNode��DataNodeʵ���ļ��Ĳ�����
 * @author DanielJyc
 *
 */
public class Client {
	NameNode namenode;
	public Client(NameNode nn) {  
		this.namenode = nn;
	}
	public void NameNodeSerial() {
        File file = new File("namenode.out");
        ObjectOutputStream oout = null;
        try {
			oout = new ObjectOutputStream(new FileOutputStream(file));
			oout.writeObject(this.namenode);
	        oout.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

//        ObjectInputStream oin = new ObjectInputStream(new FileInputStream(file));
//        Object newPerson = oin.readObject(); // û��ǿ��ת����Person����
//        oin.close();
//        System.out.println(newPerson);
	}
	/**
	 * ���ܣ�д���ļ���ͨ������DataNode��NameNode�ķ�������DataNodeд��ʵ�����ݣ���NameNodeд��Ԫ���ݡ�
	 * @param filename �ļ���
	 * @param data �ļ��ڵ�����
	 */
	public void write(String filename, String data)  {
		List<String>  chunks = new ArrayList<String> ();  //���data�ֳ�����num_chunks������
		int chunkloc = 1;
		int num_chunks = get_num_chunks(data);
		for (int i = 0; i < num_chunks; i++) { //��data����ÿ��chunk��СΪchunksize��10byte�����в��
			int low = i*namenode.chunksize;
			int high = (i+1)*namenode.chunksize > data.length()? data.length():(i+1)*namenode.chunksize;
			chunks.add((String) data.subSequence(low, high) );			
		}
		//Ϊ�ļ�����ռ䣬����Ԫ����
		List<String> chunk_uuids = namenode.alloc(filename, num_chunks);
		for (int i = 0; i < chunk_uuids.size(); i++) {
			chunkloc = i % namenode.num_datanodes + 1;
			((DataNode) namenode.datanodes.get(chunkloc)).write(chunk_uuids.get(i).toString(), chunks.get(i).toString());
			//���ݵڶ���
			chunkloc = chunkloc % namenode.num_datanodes +1;
			((DataNode) namenode.datanodes.get(chunkloc)).write(chunk_uuids.get(i).toString(), chunks.get(i).toString());
		}
	}
	
	/**
	 * ���ܣ���HDFS��ȡ�ļ���ͨ������DataNode��NameNode�ķ�������DataNode��ȡʵ�����ݣ���NameNode��ȡԪ���ݡ�
	 * @param filename
	 * @return �����ļ�����data��String��������ļ������ڣ�����DataNode��kill������"-1"��
	 */
	public String read(String filename) {
		if (true == namenode.exits(filename)) {
			String data = new String();
			List<String> chunk_uuids = (List<String> ) namenode.filetable.get(filename);
			for (Object chunk_uuid : chunk_uuids) {
				int chunkloc = (Integer) namenode.chunktable.get(chunk_uuid);
				String data_temp = ((DataNode)namenode.datanodes.get(chunkloc) ).read(chunk_uuid.toString());
				if (data_temp.equals("-1")) { //��ȡ��ǰDataNode�ϵ�chunk�����ڻ���DataNode��kill������ĳһ��DataNode���𻵡�ע��������ʱ�����Զ�ȡ���ݣ����ǲ���������-1���棩��
					System.out.println("Current chunk is broken."); 
					data_temp = ((DataNode)namenode.datanodes.get(chunkloc%namenode.num_datanodes + 1) ).read(chunk_uuid.toString());//��ȡ��һ��DataNode��chunk
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
	 * ɾ���ļ�filename
	 * @param String����filename��
	 * @return ɾ���ɹ�������true�����򷵻�false��
	 */
	public boolean delete(String filename) {
		if (true ==  namenode.exits(filename)) {
			List<String>  chunk_uuids = (List<String> ) namenode.filetable.get(filename);
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
	
	/**
	 * �г�HDFS����������ļ���
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
	 * ��ȡfilename�е�����data��chunk����
	 * @param data��filename�е�����
	 * @return filename�е�����data��chunk����
	 */
	public int get_num_chunks(String data) {
		return (int) Math.ceil((data.length()*1.0) / namenode.chunksize);
	}
	
	
}
