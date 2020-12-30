package com.example.nrt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.rabbitmq.client.Address;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class FixedEmitter {

	private static final String EXCHANGE_NAME = "MYExchange";
	private static String myRecord;

	public static void main(String[] argv) throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		Address[] addressArr = { new Address("localhost", 5672) }; // specify
																	// the IP
																	// incase
																	// queue is
																	// not on
																	// local
																	// node
																	// where
																	// this
																	// program
																	// would
																	// execute
		Connection connection = factory.newConnection(addressArr);
		Channel channel = connection.createChannel();
		String queueName = "MYQueue";
		String routingKey = "MYQueue";
		
		
		channel.exchangeDeclare(EXCHANGE_NAME, "direct");
		/*Map<String, Object> args = new HashMap<String, Object>();
		args.put("x-ha-policy", "all");*/
		channel.queueDeclare(queueName, true, false, false, null);
		channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
		String stoppedRecord;
		int i = 0;
		while (i < 1) {
			try {
				myRecord = "MY Sample record";
				channel.basicPublish(EXCHANGE_NAME, routingKey,
						MessageProperties.PERSISTENT_TEXT_PLAIN,
						myRecord.getBytes());
				System.out.println(" [x] Sent '" + myRecord + "' sent at "
						+ new Date());
				i++;
				Thread.sleep(2);
			} catch (Exception e) {
				e.printStackTrace();

			}
		}

		channel.close();
		connection.close();
	}

}
