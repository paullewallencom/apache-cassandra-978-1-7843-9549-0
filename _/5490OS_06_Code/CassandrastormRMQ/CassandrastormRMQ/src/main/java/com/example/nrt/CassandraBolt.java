package com.example.nrt;

import java.util.Map;

import me.prettyprint.cassandra.model.ConfigurableConsistencyLevel;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.HConsistencyLevel;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CassandraBolt extends BaseBasicBolt {

	/**
	 * 
	 */

	private static final long serialVersionUID = -1250807245462834151L;
	private Cluster cluster;
	private Keyspace keyspace;
	Mutator<String> stringMutator;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		CassandraHostConfigurator hostConfigurator = new CassandraHostConfigurator("localhost");
		hostConfigurator.setCassandraThriftSocketTimeout(0);

		cluster = HFactory.getOrCreateCluster("Test Cluster", hostConfigurator);
		ConfigurableConsistencyLevel consistency = new ConfigurableConsistencyLevel();
		consistency.setDefaultWriteConsistencyLevel(HConsistencyLevel.ANY);

		keyspace = HFactory.createKeyspace("testDB", cluster, consistency);
		stringMutator = HFactory.createMutator(keyspace, StringSerializer.get());
	}

	public void execute(Tuple input, BasicOutputCollector collector) {
		String test = (String) input.getValue(0);
		String[] fields = test.split("#");
		String word = fields[0];
		String count = fields[1];
		System.out.println("my word :"+word+":::: count:"+count);
		stringMutator.insert(count, "test_cf", HFactory.createColumn("word name", word, StringSerializer.get(), StringSerializer.get()));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		

	}

	public void cleanup() {
		cluster.getConnectionManager().shutdown();
	}

}
