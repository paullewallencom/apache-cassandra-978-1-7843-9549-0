package com.example.nrt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentence implements IRichBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	OutputCollector collector;
	
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2) {
		this.collector = arg2;
		
	}
	
	public void execute(Tuple arg0) {
		String messages = arg0.getStringByField("messages");
		if (null != messages){
			String[] split = messages.split(" ");
			for (String element : split){
				this.collector.emit(new Values(element));
			}
		}
		this.collector.ack(arg0);
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
      return null;
    }

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	

	

}
