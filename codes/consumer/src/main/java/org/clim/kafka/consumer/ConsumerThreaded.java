package org.clim.kafka.consumer;

import java.time.Duration;
import java.util.Properties;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.errors.WakeupException;

public class ConsumerThreaded {

    public static void main(String[] args) {
        ConsumerThreaded consumerThreaded = new ConsumerThreaded();
        consumerThreaded.run();
    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable runnable = new ConsumerRunnable(latch);
        Thread thread = new Thread(runnable);
        thread.start();
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Caught shutdown hook");
            runnable.shutdown();
            
            try {
                latch.await();
            } catch(InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            latch.await();
        } catch(InterruptedException e) {
            System.out.println("Application got interrupted");
            e.printStackTrace();
        } finally {
            System.out.println("Application is closing");
        }
    }

    class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch) {
            this.latch = latch;
        }

        public void run() {
            // 1. Create consumer properties
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "127.0.0.1:9092");
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                "my-first-app");
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                "earliest");
    
            // 2. Create a consumer instance
            consumer = new KafkaConsumer<String, String>(properties);
    
            // 3. Subscribe consumer to a topic
            consumer.subscribe(Collections.singleton("test-topic"));
    
            // 4. Poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
        
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println(
                            "Key:" + record.key() +
                            " Value: " + record.value() +
                            " Partition: " + record.partition() +
                            " Offset: " + record.offset());
                    }
                }
            } catch(WakeupException e) {
                System.out.println("Received shutdown signal");
                consumer.close();
                latch.countDown();
            }
        }
        
        public void shutdown() {
            consumer.wakeup();
        }

    }

}
