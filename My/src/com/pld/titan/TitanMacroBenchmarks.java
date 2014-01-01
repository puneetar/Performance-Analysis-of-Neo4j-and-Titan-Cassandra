package com.pld.titan;

import java.util.Iterator;
import java.util.Random;

import org.apache.commons.lang.time.StopWatch;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Text;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class TitanMacroBenchmarks {
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

	public TitanMacroBenchmarks(TitanGraph graph) {
		this.graph=graph;
	}
	
	public Iterator<Vertex> getNodesWithFilter(){
		String filter_value="34";
		StopWatch st=new StopWatch();
		st.start();
		Iterable<Vertex> v1_itr=graph.query().has(TitanBenchmark.NODE_PROPERTY,Text.CONTAINS,filter_value).vertices();
		st.stop();
		Iterator<Vertex> v_iterator=v1_itr.iterator();
		
		
		while(v_iterator.hasNext()){
			Vertex v= v_iterator.next();
			System.out.println("Node : "+v +"\t nproperty : "+v.getProperty(TitanBenchmark.NODE_PROPERTY));	
		}
		TitanBenchmark.printTime(st.toString());
	
		
		return v_iterator;
		
	}
	
	
	public Iterator<Edge> getEdgesWithFilter(){
		String filter_value="34";
		StopWatch st=new StopWatch();
		st.start();
		Iterable<Edge> e1_itr=graph.query().has(TitanBenchmark.EDGE_PROPERTY,Text.CONTAINS,filter_value).edges();
		st.stop();
		Iterator<Edge> e_iterator=e1_itr.iterator();
		
		
		while(e_iterator.hasNext()){
			Edge e= e_iterator.next();
			System.out.println("Edge : "+e +"\t eproperty : "+e.getProperty(TitanBenchmark.EDGE_PROPERTY));	
		}
		TitanBenchmark.printTime(st.toString());
	
		return e_iterator;
	}
	
	
	public boolean getKHopNeighbours(int k){
		return false;
	}
}
