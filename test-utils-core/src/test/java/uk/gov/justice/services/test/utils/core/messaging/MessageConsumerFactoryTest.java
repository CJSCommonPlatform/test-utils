package uk.gov.justice.services.test.utils.core.messaging;

import static javax.jms.Session.AUTO_ACKNOWLEDGE;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Session;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQTopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MessageConsumerFactoryTest {

    private static final String MESSAGE_SELECTOR = "ThisMessage";
    private static final String DESTINATION_NAME = "Somewhere";

    @Mock
    Session session;

    @Mock
    MessageConsumer messageConsumer;

    @Mock
    ActiveMQConnectionFactory connectionFactory;

    @Mock
    Connection connection;

    private MessageConsumerFactory messageConsumerFactory;

    @Before
    public void setup() {
        messageConsumerFactory = new MessageConsumerFactory();
    }

    @Test
    public void shouldCreateAndReturnMessageConsumer() throws JMSException {
        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(false, AUTO_ACKNOWLEDGE)).thenReturn(session);
        final ArgumentCaptor<ActiveMQTopic> topicCaptor = ArgumentCaptor.forClass(ActiveMQTopic.class);
        when(session.createConsumer(topicCaptor.capture(), eq(MESSAGE_SELECTOR))).thenReturn(messageConsumer);

        final MessageConsumer consumer = messageConsumerFactory.createAndStart(connectionFactory, MESSAGE_SELECTOR, DESTINATION_NAME);

        verify(connection).start();
        assertThat(consumer, equalTo(messageConsumer));
        assertThat(topicCaptor.getValue().getTopicName(), equalTo(DESTINATION_NAME));
    }

    @Test
    public void shouldCloseResources() throws JMSException {
        when(connectionFactory.createConnection()).thenReturn(connection);
        when(connection.createSession(false, AUTO_ACKNOWLEDGE)).thenReturn(session);
        when(session.createConsumer(any(ActiveMQTopic.class), eq(MESSAGE_SELECTOR))).thenReturn(messageConsumer);

        messageConsumerFactory.createAndStart(connectionFactory, MESSAGE_SELECTOR, DESTINATION_NAME);
        messageConsumerFactory.close();

        verify(messageConsumer).close();
        verify(session).close();
        verify(connection).close();
        verify(connectionFactory).close();
    }

    @Test
    public void shouldntAttemptToCloseNullResources() throws JMSException {
        messageConsumerFactory.close();

        verify(messageConsumer, never()).close();
        verify(session, never()).close();
        verify(connection, never()).close();
        verify(connectionFactory, never()).close();
    }

}