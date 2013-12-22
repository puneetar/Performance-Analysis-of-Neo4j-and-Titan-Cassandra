package com.pld.titan;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.configuration.BaseConfiguration;

import com.tinkerpop.rexster.client.RexProException;
import com.tinkerpop.rexster.client.RexsterClient;
import com.tinkerpop.rexster.client.RexsterClientFactory;
import com.tinkerpop.rexster.client.RexsterClientTokens;

public class RexsterImpl {

	private final static String[] arr_prop_node=new String[1000];
	private final static String[] arr_prop_edge=new String[1000];
	public final static String INPUT_CSV_FILE="/home/pld6/git/My/My/facebook-sg/newaa";

	static{
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}
	}

	public static void main(String args[]){
	RexsterClient client=null;
		try{
				client= getRexsterClientConnection();
				if(client!=null){
					loadFromCSV(client);
				}
			}
			finally{
				try {
					client.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
	}

	public static RexsterClient getRexsterClientConnection(){
		RexsterClient client=null;
		try {

			BaseConfiguration conf = new BaseConfiguration() {{
				addProperty(RexsterClientTokens.CONFIG_HOSTNAME, "192.168.0.56");
				addProperty(RexsterClientTokens.CONFIG_MESSAGE_RETRY_WAIT_MS, 0);
				addProperty(RexsterClientTokens.CONFIG_GRAPH_NAME, "graph");
			}};

			//client = RexsterClientFactory.open("192.168.0.56","graph");
			client=RexsterClientFactory.open(conf);

		} catch (RexProException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return client;
	}

	public static void loadFromCSV(RexsterClient client){
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}
		try {
			List<Map<String,Object>> result;
			//			FileReader fr = new FileReader(INPUT_CSV_FILE);
			//			BufferedReader br=new BufferedReader(fr,10240);
			//			br.readLine();
			//
			//			
			//			for(int i=1;i<=final_limit;i++){
			//				//bw_node.write(i+"\t"+arr_prop_node[new Random().nextInt(1000)]+"\n");
			//				result= client.execute("g.addVertex(['userid':'"+i+"','nproperty':'"+arr_prop_node[new Random().nextInt(1000)]+"'])");
			//				System.out.println(result);
			//			}
			//			
			//			String arr_token[]=new String[2];
			//			String line;
			//			while((line=br.readLine())!=null){
			//				arr_token=line.split(" ");
			//
			//				if(Integer.parseInt(arr_token[0])<final_limit && Integer.parseInt(arr_token[1])<final_limit){
			//					//bw_rels.write(arr_token[0]+"\t"+arr_token[1]+"\t"+"KNOWS"+
			//						//	"\t"+arr_prop_edge[new Random().nextInt(1000)]+"\n");
			//					
			//					result= client.execute("g.addEdge(g.getVertex("+arr_token[0]+"),"+
			//					"g.addEdge(g.getVertex("+arr_token[0]+")"+
			//					",'KNOWS',[eproperty:"+arr_prop_node[new Random().nextInt(1000)]+"])");
			//					System.out.println(result);
			//				}
			//			
			//			}
			//		//	result= client.execute("g.addVertex(['userid':'106','name':'divya'])");
			//		//	System.out.println(result);

			result=client.execute("vs=[] as Set;new File(\""+INPUT_CSV_FILE+"\").eachLine{l->p=l.split(\" \");vs<<p[0];vs<<p[1];}");
			//result= client.execute("g.V.userid");
			System.out.println(result);

			//vs=[] as Set;new File("/home/pld6/git/My/My/facebook-sg/facebook.100").eachLine{l->p=l.split(" ");println "${p[0]}"; vs<<p[0];vs<<p[1];}
			//vs.each{v->g.addVertex(['userid':v,'']v)}
			//	new File("/home/pld6/git/My/My/facebook-sg/facebook.100").eachLine{l->p=l.split(" ");println "adding edge between ${p[0]} : ${p[1]}";g.addEdge(g.getVertex(${p[0]}),g.getVertex(${p[1]}),'friend')}

		} catch (RexProException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
