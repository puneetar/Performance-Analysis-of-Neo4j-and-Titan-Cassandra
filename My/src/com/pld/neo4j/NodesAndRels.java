package com.pld.neo4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;

public class NodesAndRels {
	public static String INPUT_CSV_FILE="out.facebook-sg";
	public static String OUTOUT_NODE_CSV="nodes_1m.csv";
	public static String OUTOUT_RELS_CSV="rels_1m.csv";
	private static String[] arr_prop_node=new String[1000];
	private static String[] arr_prop_edge=new String[1000];

	public static void main(String[] args) {

		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}

		FileReader fr;
		try {
			fr = new FileReader(INPUT_CSV_FILE);
			BufferedReader br=new BufferedReader(fr,10240);
			br.readLine();

			File nodes=new File(OUTOUT_NODE_CSV);
			nodes.createNewFile();
			File rels=new File(OUTOUT_RELS_CSV);
			rels.createNewFile();

			FileWriter fw_node=new FileWriter(nodes);
			BufferedWriter bw_node = new BufferedWriter(fw_node,10240);
			FileWriter fw_rels=new FileWriter(rels);
			BufferedWriter bw_rels = new BufferedWriter(fw_rels,10240);

			String node_col_names="id:string:node_auto_index\tnproperty\n";
			String rels_col_names="start\tend\ttype\teproperty\n";

			bw_node.write(node_col_names);
			bw_rels.write(rels_col_names);

			String line=null;
			String [] arr_token=new String[2];

			//HashSet<RelationShips> hash_str=new HashSet<RelationShips>();
			HashSet<String> hash_str=new HashSet<String>();
			int final_limit=1000000;
			for(int i=1;i<=final_limit;i++){
				bw_node.write(i+"\t"+arr_prop_node[new Random().nextInt(1000)]+"\n");
			}

			
			RelationShips relationship;
			while((line=br.readLine())!=null){
				arr_token=line.split(" ");

//								if(!hash_str.contains(arr_token[0])){
//									bw_node.write(arr_token[0]+"\t"+arr_prop_node[new Random().nextInt(1000)]+"\n");
//									hash_str.add(arr_token[0]);
//								}
//				
//								if(!hash_str.contains(arr_token[1])){
//									bw_node.write(arr_token[1]+"\t"+arr_prop_node[new Random().nextInt(1000)]+"\n");
//									hash_str.add(arr_token[1]);
//								}
				
			//	relationship=new RelationShips(arr_token[0], arr_token[1]);

				//if(!hash_str.contains(relationship)){
				//	hash_str.add(relationship);
				if(Integer.parseInt(arr_token[0])<final_limit && Integer.parseInt(arr_token[1])<final_limit){
					bw_rels.write(arr_token[0]+"\t"+arr_token[1]+"\t"+"KNOWS"+
							"\t"+arr_prop_edge[new Random().nextInt(1000)]+"\n");
				}
			//	}
			}

			bw_node.flush();bw_rels.flush();
			bw_node.close();bw_rels.close();
			fw_node.close();fw_rels.close();

		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}

class RelationShips{
	public String from,to;
	RelationShips(String from , String to){
		this.from=from;
		this.to=to;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((from == null) ? 0 : from.hashCode());
		result = prime * result + ((to == null) ? 0 : to.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		RelationShips other = (RelationShips) obj;

		//		if (from == null) {
		//			if (other.from != null)
		//				return false;
		//		} else if (!(from.equals(other.from)||from.equals(other.to)))
		//			return false;
		//		
		//		if (to == null) {
		//			if (other.to != null)
		//				return false;
		//		} else if (!to.equals(other.to))
		//			return false;
		if(!((from.equals(other.from) && to.equals(other.to)) ||
				(from.equals(other.to) && to.equals(other.from))) )
			return false;	

		return true;
	}

}
