package com.pld.titan;

import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang.time.StopWatch;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanMicroBenchmarks {

	private TitanGraph graph=null; 
	private final static String[] arr_prop_node=new String[1000];
	private final static String[] arr_prop_edge=new String[1000];
	public static int currentNodes;
	private Random rand =new Random();

	static{
		currentNodes=BatchGraphImpl.final_limit;
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}
	}

	public TitanMicroBenchmarks(TitanGraph graph) {
		this.graph=graph;
	}


	public boolean addNode(){
		StopWatch st=new StopWatch();

		st.start();

		Vertex v=graph.addVertex(null);
		graph.commit();

		st.stop();

		v.setProperty("userid", ""+ (++currentNodes));
		v.setProperty("nproperty", arr_prop_node[rand.nextInt(1000)]);
		graph.commit();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Node Added : "+v);
		return true;
	}
	public boolean getNode(String nodeId){
		System.out.println("NodeId refered: " + nodeId );
		StopWatch st=new StopWatch();
		st.start();
		//Iterator<Vertex> v_itr=graph.query().has("userid",Compare.EQUAL,nodeId).vertices().iterator();
		Vertex v=getVertex(nodeId);
		st.stop();

		TitanBenchmark.printTime(st.toString());
		System.out.println(" Node Recieved :"+v);
		return true;

	}
	public boolean deleteNode(String nodeId){
		System.out.println("NodeId refered: " + nodeId );
		//Iterator<Vertex> v_itr=graph.query().has("userid",Compare.EQUAL,nodeId).vertices().iterator();
		Vertex v=getVertex(nodeId);
		StopWatch st=new StopWatch();
		st.start();
		Iterator<Edge> edges=v.getEdges(Direction.BOTH).iterator();
		while(edges.hasNext()){
			edges.next().remove();
		}
		v.remove();
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Node deleted : "+v);
		return true;	
	}
	
	public boolean updateNode(String nodeId){
		return updateNodeProperty(nodeId);
	}


	public boolean addNodeProperty(String nodeId){
		System.out.println("NodeId refered: " + nodeId );
		Vertex v=getVertex(nodeId);
		v.removeProperty("nproperty");
		graph.commit();

		StopWatch st=new StopWatch();
		st.start();
		v.setProperty("nproperty", arr_prop_node[rand.nextInt(1000)]);
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Node Property added for Node : "+v);
		return true;
	}

	public boolean getNodeProperty(String nodeId){
		System.out.println("NodeId refered: " + nodeId );
		//Iterator<Vertex> v_itr=graph.query().has("userid",Compare.EQUAL,nodeId).vertices().iterator();
		Vertex v=getVertex(nodeId);
		StopWatch st=new StopWatch();
		st.start();
		String prop=(String)v.getProperty("nproperty");
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("getProperty of node : "+ nodeId+" with property : "+prop);
		return true;
	}

	public boolean deleteNodeProperty(String nodeId){
		System.out.println("NodeId refered: " + nodeId );
		//Iterator<Vertex> v_itr=graph.query().has("userid",Compare.EQUAL,nodeId).vertices().iterator();
		Vertex v=getVertex(nodeId);
		StopWatch st=new StopWatch();
		st.start();
		v.removeProperty("nproperty");
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("nProperty removed of node : "+ nodeId);
		return true;
	}
	public boolean updateNodeProperty(String nodeId){
		System.out.println("NodeId refered: " + nodeId );
		//Iterator<Vertex> v_itr=graph.query().has("userid",Compare.EQUAL,nodeId).vertices().iterator();
		Vertex v=getVertex(nodeId);
		StopWatch st=new StopWatch();
		st.start();
		v.setProperty("nproperty", arr_prop_node[rand.nextInt(1000)]);
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("nProperty updated of node : "+ nodeId);
		return true;
	}


	public boolean addEdge(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		//Iterator<Vertex> v1_itr=graph.query().has("userid",Compare.EQUAL,nodes[0]).vertices().iterator();
		Vertex v1=getVertex(nodes[0]);
		//Iterator<Vertex> v2_itr=graph.query().has("userid",Compare.EQUAL,nodes[1]).vertices().iterator();
		Vertex v2=getVertex(nodes[1]);

		StopWatch st=new StopWatch();
		st.start();
		Edge e=graph.addEdge(null, v1, v2, "KNOWS");
		graph.commit();
		st.stop();

		e.setProperty("eproperty", arr_prop_edge[rand.nextInt(1000)]);
		TitanBenchmark.printTime(st.toString());
		System.out.println("Added Edge netween nodes : "+nodes[0]+" and "+nodes[1]);
		return true;

	}

	public boolean getEdge(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		//Iterator<Vertex> v1_itr=graph.query().has("userid",Compare.EQUAL,nodes[0]).vertices().iterator();
		Vertex v1=getVertex(nodes[0]);
		//Iterator<Vertex> v2_itr=graph.query().has("userid",Compare.EQUAL,nodes[1]).vertices().iterator();
		Vertex v2=getVertex(nodes[1]);

		StopWatch st=new StopWatch();
		st.start();
		Edge e=getEdge(v1, v2);
		//graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Edge between nodes : "+nodes[0]+" and "+nodes[1]+ " Edge : "+e);
		return true;

	}
	public boolean deleteEdge(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		Edge e=getEdge(getVertex(nodes[0]), getVertex(nodes[1]));
		StopWatch st=new StopWatch();
		st.start();
		e.remove();
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Edge deleted between nodes : "+nodes[0]+" and "+nodes[1]);
		return true;
	}
	public boolean updateEdge(String[] nodes){
		return updateEdgeProperty(nodes);		
	}



	public boolean addEdgeProperty(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		Edge e=getEdge(getVertex(nodes[0]), getVertex(nodes[1]));
		e.removeProperty("eproperty");
		StopWatch st=new StopWatch();
		st.start();
		e.setProperty("eproperty", arr_prop_edge[rand.nextInt(1000)]);
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Edge property between nodes : "+nodes[0]+" and "+nodes[1]);
		return true;
	}
	public boolean getEdgeProperty(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		Edge e=getEdge(getVertex(nodes[0]), getVertex(nodes[1]));

		StopWatch st=new StopWatch();
		st.start();
		String prop=(String)e.getProperty("eproperty");
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Edge property between nodes : "+nodes[0]+" and "+nodes[1] 
				+ " Edge property : "+prop);
		return true;
	}
	public boolean deleteEdgeProperty(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		Edge e=getEdge(getVertex(nodes[0]), getVertex(nodes[1]));

		StopWatch st=new StopWatch();
		st.start();
		e.removeProperty("eproperty");
		graph.commit();
		st.stop();
		e.setProperty("eproperty", arr_prop_edge[rand.nextInt(1000)]);
		TitanBenchmark.printTime(st.toString());
		System.out.println("Edge property removed between nodes : "+nodes[0]+" and "+nodes[1]);
		return true;

	}
	public boolean updateEdgeProperty(String[] nodes){
		System.out.println("Nodes refered: " + nodes[0]+" and "+nodes[1] );
		Edge e=getEdge(getVertex(nodes[0]), getVertex(nodes[1]));
		StopWatch st=new StopWatch();
		st.start();
		e.setProperty("eproperty", arr_prop_edge[rand.nextInt(1000)]);
		graph.commit();
		st.stop();
		TitanBenchmark.printTime(st.toString());
		System.out.println("Edge property updated between nodes : "+nodes[0]+" and "+nodes[1]);
		return true;

	}



	public Vertex getVertex(String nodeId){
		Iterator<Vertex> v1_itr=graph.query().has("userid",Compare.EQUAL,nodeId).vertices().iterator();
		if(v1_itr.hasNext())
			return v1_itr.next();
		else{
			String node=TitanBenchmark.getRandomNode();
			System.out.println("Vertex unavailable for Node : "+nodeId+" Using NodeId : "+node);
			return getVertex(node);
		}
			
	}

	public Edge getEdge(Vertex v1, Vertex v2){
		Iterator<Edge> edges=v1.getEdges(Direction.BOTH).iterator();
		Edge e=null;
		while(edges.hasNext()){
			e=edges.next();
			if(e.getVertex(Direction.IN).equals(v2))
				break;
		}
		return e;
	}
}
