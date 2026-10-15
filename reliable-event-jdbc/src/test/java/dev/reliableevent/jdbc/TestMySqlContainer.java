package dev.reliableevent.jdbc;

import org.testcontainers.containers.MySQLContainer;

/** Uses an explicitly configured local MySQL for integration tests, or Testcontainers by default. */
public final class TestMySqlContainer extends MySQLContainer<TestMySqlContainer> {
    private static final String URL = System.getProperty("test.mysql.url");
    private static final String USERNAME = System.getProperty("test.mysql.username");
    private static final String PASSWORD = System.getProperty("test.mysql.password");
    private static final String EFFECTIVE_USERNAME = USERNAME == null || USERNAME.isBlank() ? "test" : USERNAME;
    private static final String EFFECTIVE_PASSWORD = PASSWORD == null || PASSWORD.isBlank() ? "test" : PASSWORD;
    private final boolean external = URL != null && !URL.isBlank();

    private TestMySqlContainer(String databaseName) {
        super("mysql:8.0.36");
        withDatabaseName(databaseName).withUsername(EFFECTIVE_USERNAME).withPassword(EFFECTIVE_PASSWORD);
    }

    public static TestMySqlContainer configured(String databaseName) {
        return new TestMySqlContainer(databaseName);
    }

    @Override
    public void start() {
        if (!external) super.start();
    }

    @Override
    public void stop() {
        if (!external) super.stop();
    }

    @Override
    public String getJdbcUrl() {
        return external ? URL : super.getJdbcUrl();
    }

    @Override
    public String getUsername() {
        return external ? EFFECTIVE_USERNAME : super.getUsername();
    }

    @Override
    public String getPassword() {
        return external ? EFFECTIVE_PASSWORD : super.getPassword();
    }
}
