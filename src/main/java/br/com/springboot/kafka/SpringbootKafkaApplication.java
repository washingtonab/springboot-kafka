package br.com.springboot.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class SpringbootKafkaApplication implements CommandLineRunner {

	@Autowired
    private KafkaTemplate<String, String> template;
	
	private final CountDownLatch latch = new CountDownLatch(3);
	
	public static void main(String[] args) {
		SpringApplication.run(SpringbootKafkaApplication.class, args).close();
	}

	@Override
    public void run(String... args) throws Exception {
        this.template.send("myTopic", "foo1");
        this.template.send("myTopic", "foo2");
        this.template.send("myTopic", "foo3");
        latch.await(60, TimeUnit.SECONDS);
        System.out.println("All received");
    }

	@KafkaListener(topics = "myTopic")
    public void listen(ConsumerRecord<?, ?> cr) throws Exception {
        System.out.println(cr.toString());
        latch.countDown();
    }
	
}
