package uk.gov.justice.services.test.utils;

import static java.lang.String.format;

import uk.gov.justice.services.messaging.DefaultJsonObjectEnvelopeConverter;
import uk.gov.justice.services.messaging.JsonEnvelope;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;

import lombok.Getter;
import lombok.Setter;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.apache.commons.lang3.StringUtils;

class EventListener implements AutoCloseable {

    private static final String MESSAGE_SELECTOR_TEMPLATE = "CPPNAME IN ('%s')";

    private final String topic;

    @Getter private final List<String> events;

    @Getter @Setter private Map<String, Optional<Consumer>> handlers;

    private final long timeout;

    private final ListeningStrategy until;

    private final UUID correlationId;

    private JMSConsumer eventConsumer;

    @Getter private CompletableFuture future;

    @Getter @Setter private Runnable timeoutBehaviour;

    public EventListener(String topic, List<String> events, long timeout, ListeningStrategy until, UUID correlationId) {
        this.topic = topic;
        this.events = events;
        this.timeout = timeout;
        this.until = until;
        this.correlationId = correlationId;
    }

    public void start(JMSContext jmsContext) {
        final String messageSelector = format(MESSAGE_SELECTOR_TEMPLATE, StringUtils.join(this.events, "', '"));
        this.eventConsumer = jmsContext.createConsumer(new ActiveMQTopic(this.topic), messageSelector);
        this.eventConsumer.setMessageListener(message -> {
            try {
                String body = message.getBody(String.class);
                JsonEnvelope eventBodyEnvelope = new DefaultJsonObjectEnvelopeConverter().asEnvelope(body);

                UUID eventCorrelationId = UUID.fromString(eventBodyEnvelope.metadata().clientCorrelationId().get());
                String eventName = eventBodyEnvelope.metadata().name();

                if (!eventCorrelationId.equals(this.correlationId)) {
                    return;
                }

                Optional<Consumer> handler = handlers.get(eventName);
                handler.ifPresent(consumer -> consumer.accept(eventBodyEnvelope));

                if (ListeningStrategy.UNTIL_RECEIVAL.equals(this.until)) {
                    this.future.complete(message);
                    this.close();
                }

            } catch (JMSException e) {
                throw new RuntimeException(format("Could not receive payload for events %s", StringUtils.join(this.events, ", ")), e.getCause());
            }
        });
        this.future = initializeFuture();
    }

    @SuppressWarnings("squid:S2925")
    public CompletableFuture initializeFuture() {
        CompletableFuture<String> completableFuture = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            Thread.sleep(this.timeout);
            if( timeoutBehaviour != null ){
                timeoutBehaviour.run();
                completableFuture.complete(format("Event listener for %s successfully timed out.", StringUtils.join(events, ", ")));
            } else if (ListeningStrategy.UNTIL_RECEIVAL.equals(this.until)) {
                completableFuture.completeExceptionally(new RuntimeException(format("Event listener for %s timed out.", StringUtils.join(events, ", "))));
            } else if (ListeningStrategy.UNTIL_TIMEOUT.equals(this.until)) {
                completableFuture.complete(format("Event listener for %s timed out.", StringUtils.join(events, ", ")));
            }
            return null;
        });

        return completableFuture;
    }

    @Override
    public void close() {
        if (this.future != null && !this.future.isDone()) {
            this.future.cancel(true);
        }
        this.eventConsumer.close();
    }

}
