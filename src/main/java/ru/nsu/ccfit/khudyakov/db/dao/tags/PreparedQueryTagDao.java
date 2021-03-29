package ru.nsu.ccfit.khudyakov.db.dao.tags;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.db.dao.tags.entitiy.TagEntity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class PreparedQueryTagDao extends TagDao implements AutoCloseable {

    private static final Logger logger = LogManager.getLogger(PreparedQueryTagDao.class);

    private static final String INSERT_TAG_QUERY = "insert into tags(node_id, key, value) values (?, ?, ?)";

    protected final PreparedStatement insertStatement;

    protected final Connection connection;

    public PreparedQueryTagDao(ConnectionDataSource dataSource) {
        super(dataSource);

        connection = dataSource.getConnection();

        try {
            insertStatement = connection.prepareStatement(INSERT_TAG_QUERY);
        } catch (SQLException e) {
            logger.error("Couldn't prepare statements", e);
            throw new IllegalStateException();
        }
    }

    @Override
    public void save(TagEntity value) {
        try {
            saveTag(value);
            connection.commit();
        } catch (SQLException e) {
            logger.error("Couldn't commit query", e);
            throw new IllegalStateException();
        }
    }

    private void saveTag(TagEntity value) throws SQLException {
        try {
            insertStatement.setObject(1, value.getNodeId(), java.sql.Types.BIGINT);
            insertStatement.setString(2, value.getK());
            insertStatement.setString(3, value.getV());
            insertStatement.execute();
        } catch (SQLException e) {
            logger.error("Couldn't insert tag {}", value, e);
            throw new IllegalStateException();
        }
    }

    @Override
    public void save(List<TagEntity> values) {
        try {
            for (TagEntity value : values) {
                saveTag(value);
            }
            connection.commit();
        } catch (SQLException e) {
            logger.error("Couldn't commit batch", e);
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
