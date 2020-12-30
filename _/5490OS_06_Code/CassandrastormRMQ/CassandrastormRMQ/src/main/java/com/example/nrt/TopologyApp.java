package com.example.nrt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class TopologyApp 
{
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, InterruptedException
    {
    	
    	TopologyBuilder builder = new TopologyBuilder();
    	builder.setSpout("spout", new AMQPRecvSpout("localhost", 5672, "guest", "guest", "/", true, false), 1);

        builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 2).fieldsGrouping("split", new Fields("word"));
        builder.setBolt("cassBolt", new CassandraBolt(), 2).shuffleGrouping("count");

        Config conf = new Config();
        conf.setDebug(true);


        if (args != null && args.length > 0) {
          conf.setNumWorkers(3);

          StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
          conf.setMaxTaskParallelism(3);

          LocalCluster cluster = new LocalCluster();
          cluster.submitTopology("word-count", conf, builder.createTopology());

          try {
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
          Thread.sleep(100000);
          cluster.shutdown();
        }
        
    }
}
