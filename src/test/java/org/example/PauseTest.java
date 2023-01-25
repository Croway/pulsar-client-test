package org.example;

import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import org.testcontainers.containers.PulsarContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PauseTest {

	private final AtomicInteger counter = new AtomicInteger(0);

	@Test
	public void test() throws PulsarClientException, InterruptedException {
		PulsarContainer pulsar = new PulsarContainer(DockerImageName.parse("apachepulsar/pulsar:2.11.0"));
		pulsar.start();

		ClientBuilder builder = PulsarClient.builder();
		builder.serviceUrl(pulsar.getPulsarBrokerUrl());

		PulsarClient pulsarClient = builder.build();

		ConsumerBuilder<byte[]> consumerBuilder = pulsarClient.newConsumer();
		Consumer consumer = consumerBuilder
				.subscriptionType(SubscriptionType.Exclusive)
				.subscriptionName("camel-subscription")
				.receiverQueueSize(0)
				.consumerName("camel-consumer")
				.topic("pause-test")
				.subscribe();

		ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>());

		executorService.submit(new PulsarConsumerLoop(consumer));

		ProducerBuilder<byte[]> producerBuilder = pulsarClient.newProducer();
		Producer<byte[]> producer = producerBuilder.topic("pause-test").create();

		consumer.pause();
		producer.send("Test 1".getBytes());
		producer.send("Test 2".getBytes());

		Thread.sleep(1000); // Wait some time for the PulsarConsumerLoop..
		Assertions.assertEquals(0, counter.get());

		pulsar.stop();
	}

	class PulsarMessageListener implements MessageListener<byte[]> {

		@Override
		public void received(Consumer<byte[]> consumer, Message<byte[]> msg) {
			counter.incrementAndGet();
		}
	}

	class PulsarConsumerLoop implements Runnable {

		private Consumer<byte[]> consumer;

		public PulsarConsumerLoop(Consumer<byte[]> consumer) {
			this.consumer = consumer;
		}

		@Override
		public void run() {
			PulsarMessageListener listener = new PulsarMessageListener();
			while (true) {
				try {
					Message<byte[]> msg = consumer.receive();
					listener.received(consumer, msg);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
		}
	}
}
