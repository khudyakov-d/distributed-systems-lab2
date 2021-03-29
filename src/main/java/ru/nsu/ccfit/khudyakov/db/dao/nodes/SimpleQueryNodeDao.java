package ru.nsu.ccfit.khudyakov.db.dao.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.model.Node;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;

public class SimpleQueryNodeDao extends NodeDao {

    private static final Logger logger = LogManager.getLogger(SimpleQueryNodeDao.class);

    private static final String INSERT_NODE_QUERY =
            "insert into nodes(id, username, longitude, latitude) values ({0}, ''{1}'', {2}, {3})";

    private final Connection connection;

    public SimpleQueryNodeDao(ConnectionDataSource dataSource) {
        super(dataSource);
        connection = dataSource.getConnection();
    }


    @Override
    public void save(Node value) {
        try {
            saveNode(value);
            connection.commit();
        } catch (SQLException e) {
            logger.error("Couldn't commit query", e);
            throw new IllegalStateException();
        }
    }

    private void saveNode(Node value) {
        try (Statement statement = connection.createStatement()) {
            String query = MessageFormat.format(INSERT_NODE_QUERY,
                    value.getId().toString(),
                    value.getUser().replaceAll("'", "''"),
                    value.getLon().toString().replaceAll(",", "."),
                    value.getLat().toString().replaceAll(",", "."));
            statement.execute(query);
        } catch (SQLException e) {
            logger.error("Couldn't insert node {}", value, e);
            throw new IllegalStateException();
        }
    }

    @Override
    public void save(List<Node> values) {
        try {
            for (Node value : values) {
                saveNode(value);
            }
            connection.commit();
        } catch (SQLException e) {
            logger.error("Couldn't commit batch", e);
            throw new IllegalStateException();
        }
    }

    @Override
    public void close() throws Exception {
    }

}
