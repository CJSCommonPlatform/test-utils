package uk.gov.justice.services.test.utils;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import javax.ws.rs.HttpMethod;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface CommandClient {
    String URI();

    String contentType();

    String context() default "self";

    String httpMethod() default HttpMethod.POST;

    DeclareJointListener[] jointListeners() default {};

    ListenerConfig[] listenerConfigs() default {};
}
