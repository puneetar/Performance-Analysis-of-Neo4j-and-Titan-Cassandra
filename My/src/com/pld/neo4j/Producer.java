package com.pld.neo4j;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;

public class Producer implements Runnable {
	private final BlockingQueue<String> queue;
	Producer(BlockingQueue<String> q) { queue = q; }
	
	public void run() {

		produce(); 
		Consumer.flag=false;

	}

	void produce() {
		FileReader fr;
		try {

			fr = new FileReader(EmbeddedNeo4j.PATH_CSV_FILE);
			BufferedReader br=new BufferedReader(fr,10240);
			String[] arr_token=null;
			String line=null;
			br.readLine();

			while((line=br.readLine())!=null)
			{
				arr_token=line.split(" ");
				queue.add(arr_token[0]);
				queue.add(arr_token[1]);
			}
			br.close();fr.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}