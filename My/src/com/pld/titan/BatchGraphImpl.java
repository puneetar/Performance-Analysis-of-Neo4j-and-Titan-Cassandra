package com.pld.titan;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.cassandra.cli.CliParser.newColumnFamily_return;
import org.apache.commons.lang.time.StopWatch;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

public class BatchGraphImpl {

	static final int final_limit=5000000;
	private final static String[] arr_prop_node=new String[1000];
	private final static String[] arr_prop_edge=new String[1000];
	
	static{
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}
	}
	
	public static TitanGraph batchLoadData(String propertiesFile, String csvFile) {
		//	BaseConfiguration config = new BaseConfiguration();
		//		Configuration storage = config.subset(GraphDatabaseConfiguration.STORAGE_NAMESPACE);
		//		// storage.setProperty(GraphDatabaseConfiguration.STORAGE_CONF_FILE_KEY, arg1)
		//		// configuring local backend
		//		storage.setProperty(GraphDatabaseConfiguration.STORAGE_BACKEND_KEY, "embeddedcassandra");
		//		storage.setProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY_KEY, "cassandra.yaml");
		//		// configuring elastic search index
		//		Configuration index = storage.subset(GraphDatabaseConfiguration.INDEX_NAMESPACE).subset(INDEX_NAME);
		//		index.setProperty(INDEX_BACKEND_KEY, "elasticsearch");
		//		index.setProperty("local-mode", true);
		//		index.setProperty("client-only", false);
		//		index.setProperty(STORAGE_DIRECTORY_KEY, "db/es");

		TitanGraph graph=TitanBenchmark.getGraph(propertiesFile, true);
		BatchGraph<TitanGraph> bgraph = new BatchGraph<TitanGraph>(graph, VertexIDType.STRING, 1000);
		//bgraph.setLoadingFromScratch(false);
		//BatchGraph bgraph = BatchGraph.wrap(graph);

		try {
			FileReader fr = new FileReader(csvFile);
			BufferedReader br=new BufferedReader(fr);
			br.readLine();

			System.out.println(Thread.currentThread().getId()+" : "+csvFile);

			String line;
			String arr_token[]=new String[2];
			Vertex[] vertices;
			Edge edge;
			int j=1;
			int k=500000;
			long startTime = System.currentTimeMillis();
			StopWatch st =new StopWatch();
			st.start();
			while((line=br.readLine())!=null) {
				arr_token=line.split(" ");

				if(Integer.parseInt(arr_token[0])<final_limit && Integer.parseInt(arr_token[1])<final_limit ){
					System.out.println(Thread.currentThread().getId()+" : "+j+++" : "+line);
					if(j>k){
						st.suspend();
						k=k+500000;
						System.gc();
						System.out.println("doing system gc");
						Thread.sleep(10000);
						st.resume();
					}
					vertices = new Vertex[2];
					for (int i=0;i<2;i++) {
						vertices[i] = bgraph.getVertex(arr_token[i]);

						if (vertices[i]==null){
							vertices[i]=bgraph.addVertex(arr_token[i],"userid",arr_token[i],"nproperty", arr_prop_node[new Random().nextInt(1000)]);
							// 	vertices[i].setProperty("nproperty", arr_prop_node[new Random().nextInt(1000)]);
						}
					}
					edge = bgraph.addEdge(null,vertices[0],vertices[1],"KNOWS","eproperty",arr_prop_edge[new Random().nextInt(1000)]);
					//edge.setProperty("eproperty",arr_prop_edge[new Random().nextInt(1000)]);
				}
			}
			bgraph.commit();
			graph.commit();
			st.stop();
			System.out.println("Time taken : " + st.toString());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		return graph;
	}

}
