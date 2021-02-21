package uk.gov.justice.services.test.utils;

import static java.lang.String.format;
import uk.gov.justice.services.common.converter.jackson.ObjectMapperProducer;
import uk.gov.justice.services.common.http.HeaderConstants;
import uk.gov.justice.services.test.utils.core.http.BaseUriProvider;
import uk.gov.justice.services.test.utils.core.messaging.QueueUriProvider;
import uk.gov.justice.services.test.utils.core.rest.RestClient;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidatorFactory;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;

import com.damnhandy.uri.template.UriTemplate;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.PropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import com.google.common.base.Strings;
import com.google.common.io.Resources;
import lombok.Getter;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

public class CommandExecutor {

    public static final String PUBLIC_ACTIVE_MQ_TOPIC = "public.event";
    public static final String PRIVATE_ACTIVE_MQ_TOPIC = "sjp.event";
    public static final Integer MESSAGE_QUEUE_TIMEOUT = 10000;

    private static final String QUEUE_URI = QueueUriProvider.queueUri();

    private static final RestClient restClient = new RestClient();
    private static final ObjectMapper mapper = new ObjectMapperProducer().objectMapper().copy();
    private static final ValidatorFactory validatorFactory = Validation.buildDefaultValidatorFactory();

    private Object commandInstance;
    @Getter private UUID correlationId = UUID.randomUUID();
    @Getter private UUID userId = UUID.randomUUID();
    @Getter private Consumer<Set<ConstraintViolation<Object>>> onInvalidCommand;
    private Map<String, EventListener> listeners = new HashMap<>();

    public CommandExecutor(Object commandInstance) {
        this.commandInstance = commandInstance;
        this.processCommandDeclaration(commandInstance);
    }

    public CommandExecutor setCorrelationId(UUID correlationId) {
        this.correlationId = correlationId;
        return this;
    }

    public CommandExecutor setExecutingUserId(UUID userId) {
        this.userId = userId;
        return this;
    }

    public CommandExecutor onInvalid(Consumer<Set<ConstraintViolation<Object>>> onInvalidCommand) {
        this.onInvalidCommand = onInvalidCommand;
        return this;
    }

    public CommandExecutor setTimeoutFor(String key, Runnable timeout) {
        if (listeners.containsKey(key)) {
            listeners.get(key).setTimeoutBehaviour(timeout);
        } else {
            throw new IllegalArgumentException(format("Event listener key %s not present", key));
        }
        return this;
    }

    public Optional<Response> execute() {
        if (!validateCommandInstance()) {
            return Optional.empty();
        }

        String httpMethod = getCommandHttpMethod();
        String commandUri = getCommandUri();
        String commandContentType = getCommandContentType();
        String payload = getPayload();

        startListeners();
        Response response = makeHttpCall(httpMethod, commandUri, commandContentType, payload);
        if( response.getStatusInfo().getFamily() != Response.Status.Family.SUCCESSFUL ){
            stopListeners();
        }
        return Optional.of(response);
    }

    public Optional<Response> executeSync() {
        Optional<Response> response = execute();
        await();
        return response;
    }

    public CommandExecutor await() {
        try {
            CompletableFuture[] futures = this.listeners
                    .values().stream().filter((listener) -> listener.getFuture() != null)
                    .map(EventListener::getFuture)
                    .toArray(CompletableFuture[]::new);
            CompletableFuture<Void> combinedFuture = CompletableFuture.allOf(futures);
            combinedFuture.get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return this;
    }

    public CommandExecutor awaitFor(String key) {
        try {
            this.listeners.get(key).getFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(format("Event listener with the key %s for command %s cancelled", key, this.commandInstance.getClass().getName()), e);
        }
        return this;
    }

    private void processCommandDeclaration(Object commandInstance) {
        if ( !commandInstance.getClass().isAnnotationPresent(CommandClient.class)) {
            throw new IllegalArgumentException("Command executor can only be used with the classes decorated with uk.gov.justice.services.test.utils.CommandClient annotation.");
        }

        validateEventHandlersAreConsumers(commandInstance);

        List<String> eventNames = getDeclaredEventNames(commandInstance);
        List<String> remainingEvents = new ArrayList<>(eventNames);

        DeclareJointListener[] jointListenersConfigs = commandInstance.getClass().getAnnotation(CommandClient.class).jointListeners();
        Map<String, ListenerConfig> listenerConfigs = Arrays.stream(commandInstance.getClass().getAnnotation(CommandClient.class).listenerConfigs())
                .collect(Collectors.toMap(ListenerConfig::key, Function.identity()));

        configureJointListeners(eventNames, remainingEvents, jointListenersConfigs, listenerConfigs);
        configureSingleListeners(remainingEvents, listenerConfigs);
    }

    private void validateEventHandlersAreConsumers(Object commandInstance) {
        Arrays
                .stream(commandInstance.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(EventHandler.class))
                .forEach(field -> {
                    if (!field.getGenericType().equals(Consumer.class)) {
                        throw new RuntimeException("Event Handler annotation can only be used on Consumer type fields");
                    }
                });
    }

    private List<String> getDeclaredEventNames(Object commandInstance) {
        return Arrays
                    .stream(commandInstance.getClass().getDeclaredFields())
                    .filter(field -> field.isAnnotationPresent(EventHandler.class))
                    .map(field -> field.getAnnotation(EventHandler.class).value())
                    .collect(Collectors.toList());
    }

    private void configureSingleListeners(List<String> remainingEvents, Map<String, ListenerConfig> listenerConfigs) {
        for (String event : remainingEvents) {
            ListenerConfig listenerConfig = listenerConfigs.get(event);
            final long timeout = listenerConfig != null ? listenerConfig.timeout() : MESSAGE_QUEUE_TIMEOUT;
            final ListeningStrategy until  = listenerConfig != null ? listenerConfig.until() : ListeningStrategy.UNTIL_RECEIVAL;
            EventListener listener = new EventListener(determineTopic(event), Arrays.asList(event), timeout, until, this.correlationId);
            this.listeners.put(event, listener);
        }
    }

    private void configureJointListeners(List<String> eventNames, List<String> remainingEvents, DeclareJointListener[] jointListenersConfigs, Map<String, ListenerConfig> listenerConfigs) {
        Arrays.stream(jointListenersConfigs).forEach((config) -> {
            if (this.listeners.containsKey(config.key())) {
                throw new RuntimeException("uk.gov.justice.services.test.utils.EventListener key already declared for command client");
            }

            boolean allEventsPresent = Arrays.stream(config.events()).allMatch(eventNames::contains);
            if (!allEventsPresent) {
                throw new RuntimeException("Not all declared events in the joint listener are handled by the command client");
            }

            remainingEvents.removeAll(Arrays.asList(config.events()));

            ListenerConfig listenerConfig = listenerConfigs.get(config.key());
            final long timeout = listenerConfig != null ? listenerConfig.timeout() : MESSAGE_QUEUE_TIMEOUT;
            final ListeningStrategy until = listenerConfig != null ? listenerConfig.until() : ListeningStrategy.UNTIL_RECEIVAL;
            EventListener listener = new EventListener(config.topic(), Arrays.asList(config.events()), timeout, until, this.correlationId);
            this.listeners.put(config.key(), listener);
        });
    }

    private boolean validateCommandInstance() {
        Set<ConstraintViolation<Object>> violations = validatorFactory.getValidator().validate(this.commandInstance);
        if (violations.size() == 0) {
            return true;
        } else if (this.onInvalidCommand != null) {
            onInvalidCommand.accept(violations);
            return false;
        } else {
            throw new RuntimeException("Command instance is not valid: " + violations);
        }
    }

    private String getCommandUri() {
        CommandClient annotation = commandInstance.getClass().getAnnotation(CommandClient.class);
        UriTemplate template = UriTemplate.buildFromTemplate(annotation.URI()).build();
        String context = annotation.context();

        Properties clientProperties = new Properties();
        try {
            String propertyFilePath = Resources.getResource("client.properties").getPath();
            clientProperties.load(new FileInputStream(propertyFilePath));
        } catch (IOException e) {
            throw new RuntimeException("Could not find 'client.properties' file under resources to get base endpoint paths ", e);
        }

        String baseUri =  BaseUriProvider.getBaseUri();
        String writeBaseUri = baseUri + clientProperties.getProperty(format("%s.commandBase", context));

        try {
            for (String variable : template.getVariables()) {
                Object value = commandInstance.getClass().getField(variable).get(commandInstance);
                template.set(variable, value);
            }
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException("Uri template can not be expanded.");
        }

        return writeBaseUri + template.expand();
    }

    private String getCommandContentType() {
        CommandClient annotation = commandInstance.getClass().getAnnotation(CommandClient.class);
        return annotation.contentType();
    }

    private String getCommandHttpMethod() {
        CommandClient annotation = commandInstance.getClass().getAnnotation(CommandClient.class);
        return annotation.httpMethod();
    }

    @JsonFilter("defaultFilter")
    private static class CommandMixin {
    }

    private String getPayload() {
        try {
            String[] nonEventHandlers = Arrays.stream(commandInstance.getClass().getDeclaredFields())
                    .filter(field -> field.isAnnotationPresent(EventHandler.class))
                    .map(Field::getName)
                    .toArray(String[]::new);

            PropertyFilter filterOutEventHandlers = SimpleBeanPropertyFilter.serializeAllExcept(nonEventHandlers);
            SimpleFilterProvider filterProvider = new SimpleFilterProvider().addFilter("defaultFilter", filterOutEventHandlers);

            mapper.setVisibility(PropertyAccessor.GETTER, JsonAutoDetect.Visibility.NONE);
            mapper.addMixIn(Object.class, CommandMixin.class);
            mapper.setFilterProvider(filterProvider);

            return mapper.writeValueAsString(this.commandInstance);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(format("Could not serialize command client %s instance", this.commandInstance.getClass().getName()), e);
        }
    }

    private void startListeners() {
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(QUEUE_URI);
        JMSContext jmsContext = connectionFactory.createContext();
        getListenersToStart().forEach((listener) -> {
            listener.setHandlers(this.getHandlers(listener.getEvents()));
            listener.start(jmsContext);
        });
    }

    private void stopListeners() {
        this.listeners
                .values().stream().filter((listener) -> listener.getFuture() != null)
                .forEach(EventListener::close);
    }

    private List<EventListener> getListenersToStart() {
        return this.listeners
                .values().stream()
                .filter((listener) -> listener.getEvents().stream().anyMatch((event) -> {
                    try {
                        boolean handlerProvided = null != getHandlerFieldFor(event).get(this.commandInstance);
                        boolean timeoutProvided = null != listener.getTimeoutBehaviour();
                        return handlerProvided || timeoutProvided;
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                        return false;
                    }
                })).collect(Collectors.toList());
    }

    private Response makeHttpCall(String method, String url, String mediaType, String payload) {
        MultivaluedMap<String, Object> map = new MultivaluedHashMap<>();
        map.add(HeaderConstants.USER_ID, userId.toString());
        map.add(HeaderConstants.CLIENT_CORRELATION_ID, correlationId);

        if (HttpMethod.POST.equals(method)) {
            return restClient.postCommand(url, mediaType, payload, map);
        } else if (HttpMethod.DELETE.equals(method)) {
            return restClient.deleteCommand(url, mediaType, map);
        } else {
            throw new RuntimeException(format("Http method %s not supported", method));
        }
    }

    private Field getHandlerFieldFor(String event) {
        return Arrays
                .stream(commandInstance.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(EventHandler.class))
                .filter(field -> field.getAnnotation(EventHandler.class).value().equals(event))
                .findFirst()
                .get();
    }

    private Optional<Consumer> getHandlerFor(String event) {
        try {
            return Optional.of((Consumer) getHandlerFieldFor(event).get(this.commandInstance));
        } catch (IllegalAccessException | NullPointerException e) {
            return Optional.empty();
        }
    }

    private Map<String, Optional<Consumer>> getHandlers(List<String> events) {
        return events.stream().collect(Collectors.toMap(Function.identity(), this::getHandlerFor));
    }

    private static String determineTopic(String eventName) {
        if (Strings.isNullOrEmpty(eventName)) {
            throw new RuntimeException(format("Event topic for %s could not be determined", eventName));
        } else if (eventName.startsWith("public")) {
            return PUBLIC_ACTIVE_MQ_TOPIC;
        } else if (eventName.contains(PRIVATE_ACTIVE_MQ_TOPIC)) {
            return PRIVATE_ACTIVE_MQ_TOPIC;
        } else {
            throw new RuntimeException(format("Event topic for %s could not be determined", eventName));
        }
    }

}
