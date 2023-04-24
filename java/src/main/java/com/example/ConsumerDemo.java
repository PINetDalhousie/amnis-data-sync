package com.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.logging.*;
import java.util.regex.Pattern;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = Logger.getLogger(ConsumerDemo.class.getName());    
    private static List<String> topics = new ArrayList<>();  
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");  

    public static void main(String[] args) {        
        String nodeId = args[0];
        int nTopics = Integer.parseInt(args[1]);
	    String fetchMinBytes = args[2];
	    String fetchMaxWait = args[3];
	    String sessionTimeout = args[4];

        String bootstrapServers = "10.0.0." + nodeId + ":9092";
        String groupId = "group-" + nodeId;
        String rackId = "rack-" + nodeId;
        String logFilename = "cons-" + nodeId + ".log";

        // create log handler
        createLogHandler(logFilename);

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        properties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWait);
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeout);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.CLIENT_RACK_CONFIG, rackId);                

		//consumer_timeout_ms=timeout,

        // create consumer
        log.info("Creating Consumer for bootstrapServer " + bootstrapServers);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Add shutdown hook
        addShutdownHook(consumer);

        try {
            // subscribe consumer to our topic(s)
            for(int i = 0; i < nTopics; i++){
                topics.add("topic-" + Integer.toString(i));
            }
            consumer.subscribe(topics);
            log.info("Subscribed to topics");


            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    //log.info(dateFormat.format(new Date())+" New Record");
                    processMessage(record);
                }
            }
        } catch (Exception e) {
            log.severe("Exception: " + e);
        } finally {
            consumer.close(); // this will also commit the offsets if need be.
            log.info("The consumer is now gracefully closed.");
        }

    }

    private static void createLogHandler(String filename) {
        FileHandler fh;
        try {
            fh = new FileHandler(filename);
            log.addHandler(fh);
            //SimpleFormatter formatter = new SimpleFormatter();
            SimpleFormatter formatter = new SimpleFormatter() {
                @Override
                public String format(java.util.logging.LogRecord record) {
                    return record.getMessage() + System.lineSeparator();
                }
            };
            fh.setFormatter(formatter);
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private static void addShutdownHook(KafkaConsumer<String, String> consumer) {
        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private static void processMessage(ConsumerRecord<String, String> record) {
        String msgContent = record.value();
        String prodId = msgContent.substring(0, 2);
        String msgId = msgContent.substring(2, 8);        
        String formattedDate = dateFormat.format(new Date());        
        log.info(formattedDate + " INFO:Prod ID: " + prodId + "; Message ID: " + msgId + "; Latest: False; Topic: " + record.topic() +"; Offset: " + record.offset() + "; Size: " + record.serializedValueSize());          
    }
}