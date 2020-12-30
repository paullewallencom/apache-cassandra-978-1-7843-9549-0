package com.example.nrt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCount extends BaseBasicBolt{
	Map<String, Integer> counts = new HashMap<String, Integer>();

    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      System.out.println("word= "+word+": count ="+count);
      collector.emit(new Values(word+"#"+count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
     // declarer.declare(new Fields("word", "count"));
    	 declarer.declare(new Fields("word"));
    }

}
