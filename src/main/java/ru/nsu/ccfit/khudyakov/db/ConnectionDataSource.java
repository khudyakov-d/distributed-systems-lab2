package ru.nsu.ccfit.khudyakov.db;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionDataSource implements AutoCloseable {

    public static final String DATABASE_URL = "jdbc:postgresql://localhost:5432/db";

    public static final String DATABASE_USERNAME = "root";

    public static final String DATABASE_PASSWORD = "root";

    private static final Logger logger = LogManager.getLogger(ConnectionDataSource.class);

    private final Connection connection;

    public ConnectionDataSource() {
        try {
            connection = DriverManager.getConnection(DATABASE_URL, DATABASE_USERNAME, DATABASE_PASSWORD);
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("Couldn't open connection", e);
            throw new IllegalStateException();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            connection.close();
        } catch (SQLException e) {
            logger.error("Couldn't close connection", e);
            throw new IllegalStateException();
        }
    }

    public Connection getConnection() {
        return connection;
    }

}
