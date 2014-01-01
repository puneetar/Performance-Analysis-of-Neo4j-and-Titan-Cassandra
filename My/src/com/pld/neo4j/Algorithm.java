package com.pld.neo4j;

import java.util.Comparator;
import java.util.Iterator;

import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.CostAccumulator;
import org.neo4j.graphalgo.CostEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.Traversal;

public class Algorithm {
	Node startNode,endNode;

	public void shortestPath1(String nodeId1,String nodeId2){
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			startNode=EmbeddedNeo4j.index.get("id", nodeId1).getSingle();
			endNode=EmbeddedNeo4j.index.get("id", nodeId2).getSingle();
			org.neo4j.graphalgo.impl.shortestpath.Dijkstra<Double> dijkstra =
					new org.neo4j.graphalgo.impl.shortestpath.Dijkstra<Double>(0d, startNode, endNode,
							new CostEvaluator<Double> () {	
						@Override
						public Double getCost(Relationship rel, Direction dir) {
							return 1d;
						}
					}, new CostAccumulator<Double>() {
						@Override
						public Double addCosts(Double a, Double b) {
							return a.doubleValue() + b.doubleValue();
						}
					}, new Comparator<Double>() {
						@Override
						public int compare(Double a, Double b) {
							if (a < b)
								return -1;
							if (a > b)
								return 1;
							return 0;	
						}
					}, Direction.BOTH, EmbeddedNeo4j.RelTypes.KNOWS);
			dijkstra.calculate();
			System.out.println("Cost of path is: " + dijkstra.getCost());
			tx.success();
		}
	}

	public void shortestPath(String nodeId1,String nodeId2){
		final RelationshipExpander expander;
		
		final PathFinder<WeightedPath> dijkstraPathFinder;
		try ( Transaction tx = EmbeddedNeo4j.graphDb.beginTx() ){
			expander = Traversal.expanderForTypes(
					EmbeddedNeo4j.RelTypes.KNOWS, Direction.BOTH );
			//costEvaluator = CommonEvaluators.doubleCostEvaluator( "1" );
			startNode=EmbeddedNeo4j.index.get("id", nodeId1).getSingle();
			endNode=EmbeddedNeo4j.index.get("id", nodeId2).getSingle();
			CostEvaluator<Double> costEvaluator= new CostEvaluator<Double>() {

				@Override
				public Double getCost(Relationship rel, Direction dir) {
					// TODO Auto-generated method stub
					return 1d;
				}
			};
			dijkstraPathFinder = GraphAlgoFactory.dijkstra( expander, costEvaluator);
			//WeightedPath path = dijkstraPathFinder.findSinglePath( startNode, endNode );
			Iterable<WeightedPath> itrweight=dijkstraPathFinder.findAllPaths(startNode, endNode);
			Iterator<WeightedPath> itr=itrweight.iterator();
			System.out.println("m here");
		
			while(itr.hasNext()){
				System.out.println("m here1");
				WeightedPath p=itr.next();
				System.out.println("m here2");
				System.out.println(p.weight());
			}
//			System.out.println(path.weight());
//
//			for ( Node node : path.nodes() )
//			{
//				System.out.println( node.getProperty( "id" ) );
//			}
			tx.success();
		}
	}

}
