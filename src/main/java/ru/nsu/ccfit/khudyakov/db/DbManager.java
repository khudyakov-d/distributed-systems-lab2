package ru.nsu.ccfit.khudyakov.db;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@RequiredArgsConstructor
public class DbManager {

    private static final Logger logger = LogManager.getLogger(ConnectionDataSource.class);

    private static final String TRUNCATE_NODES_TABLE = "truncate nodes cascade";

    private final ConnectionDataSource dataSource;

    public void truncateNodeTable() {
        try {
            Connection connection = dataSource.getConnection();
            Statement statement = connection.createStatement();
            statement.execute(TRUNCATE_NODES_TABLE);
        } catch (SQLException e) {
            logger.error("Couldn't truncate node table", e);
            throw new IllegalStateException();
        }
    }

}
