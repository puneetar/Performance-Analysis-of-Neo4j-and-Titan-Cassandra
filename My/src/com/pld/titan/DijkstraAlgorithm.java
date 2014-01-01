package com.pld.titan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.lang.time.StopWatch;

import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

public class DijkstraAlgorithm {

	private TitanGraph graph=null; 
	private final static String[] arr_prop_node=new String[1000];
	private final static String[] arr_prop_edge=new String[1000];
	public static int currentNodes;
	private Random rand =new Random();
	private final List<Vertex> nodes;
	private final List<Edge> edges;
	private Set<Vertex> settledNodes;
	private Set<Vertex> unSettledNodes;
	private Map<Vertex, Vertex> predecessors;
	private Map<Vertex, Integer> distance;

	static{
		currentNodes=BatchGraphImpl.final_limit;
		for(int i=0;i<1000;i++){
			arr_prop_node[i]="np"+i;
			arr_prop_edge[i]="ep"+i;
		}
	}

	public DijkstraAlgorithm(TitanGraph graph) {
		this.graph=graph;
		// create a copy of the array so that we can operate on this array
		System.out.println("loading all vertices");
		ArrayList<Vertex> arr_v=new ArrayList<Vertex>();
		Iterator<Vertex> v1_itr=graph.getVertices().iterator();
		
		int i =0;
		while(v1_itr.hasNext()){
			System.out.print(i+++" ");
			arr_v.add(v1_itr.next());
		}

		i=0;
		System.out.println("loading all edges");
		ArrayList<Edge> arr_e=new ArrayList<Edge>();
		Iterator<Edge> e1_itr=graph.getEdges().iterator();
		while(e1_itr.hasNext()){
			System.out.print(i+++" ");
			arr_e.add(e1_itr.next());
		}
		this.nodes = arr_v;
		this.edges = arr_e;
	}

	public void execute(Vertex source) {
		settledNodes = new HashSet<Vertex>();
		unSettledNodes = new HashSet<Vertex>();
		distance = new HashMap<Vertex, Integer>();
		predecessors = new HashMap<Vertex, Vertex>();
		distance.put(source, 0);
		unSettledNodes.add(source);
		while (unSettledNodes.size() > 0) {
			Vertex node = getMinimum(unSettledNodes);
			settledNodes.add(node);
			unSettledNodes.remove(node);
			findMinimalDistances(node);
		}
	}

	private void findMinimalDistances(Vertex node) {
		List<Vertex> adjacentNodes = getNeighbors(node);
		for (Vertex target : adjacentNodes) {
			if (getShortestDistance(target) > getShortestDistance(node)
					+ getDistance(node, target)) {
				distance.put(target, getShortestDistance(node)
						+ getDistance(node, target));
				predecessors.put(target, node);
				unSettledNodes.add(target);
			}
		}

	}

	private int getDistance(Vertex node, Vertex target) {
		for (Edge edge : edges) {
			if (edge.getVertex(Direction.OUT).equals(node)
					&& edge.getVertex(Direction.IN).equals(target)) {
				return 1;
			}
		}
		throw new RuntimeException("Should not happen");
	}

	private List<Vertex> getNeighbors(Vertex node) {
		List<Vertex> neighbors = new ArrayList<Vertex>();
		for (Edge edge : edges) {
			if (edge.getVertex(Direction.OUT).equals(node)
					&& !isSettled(edge.getVertex(Direction.IN))) {
				neighbors.add(edge.getVertex(Direction.IN));
			}
		}
		return neighbors;
	}

	private Vertex getMinimum(Set<Vertex> vertexes) {
		Vertex minimum = null;
		for (Vertex vertex : vertexes) {
			if (minimum == null) {
				minimum = vertex;
			} else {
				if (getShortestDistance(vertex) < getShortestDistance(minimum)) {
					minimum = vertex;
				}
			}
		}
		return minimum;
	}

	private boolean isSettled(Vertex vertex) {
		return settledNodes.contains(vertex);
	}

	private int getShortestDistance(Vertex destination) {
		Integer d = distance.get(destination);
		if (d == null) {
			return Integer.MAX_VALUE;
		} else {
			return d;
		}
	}

	/*
	 * This method returns the path from the source to the selected target and
	 * NULL if no path exists
	 */
	public LinkedList<Vertex> getPath(Vertex target) {
		LinkedList<Vertex> path = new LinkedList<Vertex>();
		Vertex step = target;
		// check if a path exists
		if (predecessors.get(step) == null) {
			return null;
		}
		path.add(step);
		while (predecessors.get(step) != null) {
			step = predecessors.get(step);
			path.add(step);
		}
		// Put it into the correct order
		Collections.reverse(path);
		return path;
	}
	
	public LinkedList<Vertex> getShortestPath(String source, String target){
		System.out.println("Nodes refered: " + source+" and "+target );
		
	    Vertex source1=TitanMicroBenchmarks.getVertex(source);
	    Vertex target1=TitanMicroBenchmarks.getVertex(target);
		
	    StopWatch st=new StopWatch();
		st.start();
		DijkstraAlgorithm dijkstra = new DijkstraAlgorithm(graph);
		dijkstra.execute(source1);
	    LinkedList<Vertex> path = dijkstra.getPath(target1);
	    st.stop();
	    System.out.println(path);
	    TitanBenchmark.printTime(st.toString());
		return path;
	}
}
