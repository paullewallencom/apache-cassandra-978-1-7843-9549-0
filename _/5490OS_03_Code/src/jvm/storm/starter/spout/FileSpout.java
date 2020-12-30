package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Random;

public class FileSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  
  DataInputStream in;
	BufferedReader br;
	Queue qe;
	
	public FileSpout() {
		qe = new LinkedList();
	}

	private String getMsgId(int i) {
		return (new StringBuilder("#@#MsgId")).append(i).toString();
	}
	
	private void queueIt() {
		int msgId = 0;
		String strLine;
		try {
			while ((strLine = br.readLine()) != null) {
				qe.add((new StringBuilder(String.valueOf(strLine))).append(getMsgId(msgId)).toString());
				msgId++;
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void readFile() {
		try {
			FileInputStream fstream = new FileInputStream("/home/impadmin/mylog");
			in = new DataInputStream(fstream);
			br = new BufferedReader(new InputStreamReader(in));
			queueIt();
			System.out.println("FileSpout file reading done");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    readFile();
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    
    String fullMsg = (String) qe.poll();
	String msg[] = (String[]) null;
	if (fullMsg != null) {
		msg = (new String(fullMsg)).split("#@#");
		_collector.emit(new Values(msg[0]));
		System.out.println((new StringBuilder("nextTuple done ")).append(msg[1]).toString());
	} else {
		readFile();
	}
	
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}