package dev.reliableevent.jdbc.fault;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

record PublicationProcessConfig(
        String jdbcUrl,
        String username,
        String password,
        String workerId,
        Duration leaseDuration,
        Instant claimNow
) {

    static final String JDBC_URL_ENV = "RELIABLE_EVENT_FAULT_JDBC_URL";
    static final String USERNAME_ENV = "RELIABLE_EVENT_FAULT_USERNAME";
    static final String PASSWORD_ENV = "RELIABLE_EVENT_FAULT_PASSWORD";
    static final String WORKER_ID_ENV = "RELIABLE_EVENT_FAULT_WORKER_ID";
    static final String LEASE_MILLIS_ENV = "RELIABLE_EVENT_FAULT_LEASE_MILLIS";
    static final String CLAIM_NOW_ENV = "RELIABLE_EVENT_FAULT_CLAIM_NOW";

    PublicationProcessConfig {
        jdbcUrl = required(jdbcUrl, "jdbcUrl");
        username = required(username, "username");
        if (password == null) {
            throw new NullPointerException("password must not be null");
        }
        workerId = required(workerId, "workerId");
        if (workerId.codePointCount(0, workerId.length()) > 128) {
            throw new IllegalArgumentException("workerId must not exceed 128 characters");
        }
        if (leaseDuration == null) {
            throw new NullPointerException("leaseDuration must not be null");
        }
        if (leaseDuration.toMillis() <= 0) {
            throw new IllegalArgumentException("leaseDuration must be positive");
        }
        if (claimNow == null) {
            throw new NullPointerException("claimNow must not be null");
        }
    }

    static PublicationProcessConfig fromEnvironment(Map<String, String> environment) {
        if (environment == null) {
            throw new NullPointerException("environment must not be null");
        }
        String jdbcUrl = required(environment.get(JDBC_URL_ENV), JDBC_URL_ENV);
        String username = required(environment.get(USERNAME_ENV), USERNAME_ENV);
        String password = present(environment, PASSWORD_ENV);
        String workerId = required(environment.get(WORKER_ID_ENV), WORKER_ID_ENV);
        String leaseMillis = required(environment.get(LEASE_MILLIS_ENV), LEASE_MILLIS_ENV);
        String claimNow = required(environment.get(CLAIM_NOW_ENV), CLAIM_NOW_ENV);
        try {
            return new PublicationProcessConfig(
                    jdbcUrl,
                    username,
                    password,
                    workerId,
                    Duration.ofMillis(Long.parseLong(leaseMillis)),
                    Instant.parse(claimNow)
            );
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(LEASE_MILLIS_ENV + " must be a number", exception);
        } catch (java.time.format.DateTimeParseException exception) {
            throw new IllegalArgumentException(CLAIM_NOW_ENV + " must be an instant", exception);
        }
    }

    void applyTo(Map<String, String> environment) {
        environment.put(JDBC_URL_ENV, jdbcUrl);
        environment.put(USERNAME_ENV, username);
        environment.put(PASSWORD_ENV, password);
        environment.put(WORKER_ID_ENV, workerId);
        environment.put(LEASE_MILLIS_ENV, Long.toString(leaseDuration.toMillis()));
        environment.put(CLAIM_NOW_ENV, claimNow.toString());
    }

    private static String required(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }

    private static String present(Map<String, String> environment, String name) {
        if (!environment.containsKey(name) || environment.get(name) == null) {
            throw new IllegalArgumentException(name + " must be present");
        }
        return environment.get(name);
    }
}
