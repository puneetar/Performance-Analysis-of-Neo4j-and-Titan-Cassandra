package com.pld.titan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.attribute.Text;
import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Direction;
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
		Iterator<Vertex> v1_itr=graph.query().has(TitanBenchmark.NODE_PROPERTY,Text.CONTAINS,filter_value).vertices().iterator();
		return v1_itr;
		
	}
	
	
	public Iterator<Edge> getEdgesWithFilter(){
		String filter_value="34";
		Iterator<Edge> e1_itr=graph.query().has(TitanBenchmark.EDGE_PROPERTY,Text.CONTAINS,filter_value).edges().iterator();
		return e1_itr;
	}

	public ArrayList<Vertex> getNeighbours(Vertex v1){
		ArrayList<Vertex> arls=new ArrayList<Vertex>();
		Iterator<Edge> edges=v1.getEdges(Direction.BOTH).iterator();
		while(edges.hasNext()){
			Edge e=edges.next();
			arls.add(e.getVertex(Direction.BOTH));
		}
		return arls;
	}

	public boolean getKHopNeighbours(int k,String startnode){
		Vertex v1=TitanMicroBenchmarks.getVertex(startnode);
		ArrayList<Vertex> arls=new ArrayList<Vertex>();
		arls.add(v1);
		Vertex v=v1;
		while(k!=0){
			int size=arls.size();
			for(int i=0;i<size;i++){
				arls.addAll(getNeighbours(arls.get(i)));
			}
			k--;
		}
		return false;
	}
}
