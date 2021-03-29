package ru.nsu.ccfit.khudyakov.db.dao.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.model.Node;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedQueryNodeDao extends NodeDao {

    private static final Logger logger = LogManager.getLogger(PreparedQueryNodeDao.class);

    private static final String INSERT_NODE_QUERY = "insert into nodes(id, username, longitude, latitude) values (?, ?, ?, ?)";

    protected final PreparedStatement insertStatement;

    protected final Connection connection;

    public PreparedQueryNodeDao(ConnectionDataSource dataSource) {
        super(dataSource);
        connection = dataSource.getConnection();

        try {
            insertStatement = connection.prepareStatement(INSERT_NODE_QUERY);
        } catch (SQLException e) {
            logger.error("Couldn't prepare statements", e);
            throw new IllegalStateException();
        }
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

    @Override
    public void save(List<Node> values) {
        try {
            for (Node value : values) {
                saveNode(value);
            }
            connection.commit();
        } catch (SQLException e) {
            logger.error("Couldn't commit query", e);
            throw new IllegalStateException();
        }

    }

    private void saveNode(Node value) {
        try {
            insertStatement.setObject(1, value.getId(), java.sql.Types.BIGINT);
            insertStatement.setString(2, value.getUser());
            insertStatement.setDouble(3, value.getLon());
            insertStatement.setDouble(4, value.getLat());
            insertStatement.execute();
        } catch (SQLException e) {
            logger.error("Couldn't insert node {}", value, e);
            throw new IllegalStateException();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            insertStatement.close();
        } catch (SQLException e) {
            logger.error("Couldn't close statements ", e);
            throw new IllegalStateException();
        }
    }

}
