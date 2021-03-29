package ru.nsu.ccfit.khudyakov.db.dao.tags;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.db.dao.tags.entitiy.TagEntity;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.List;

public class SimpleQueryTagDao extends TagDao {

    private static final Logger logger = LogManager.getLogger(SimpleQueryTagDao.class);

    private static final String INSERT_TAG_QUERY = "insert into tags(node_id, key, value) values ({0}, ''{1}'', ''{2}'')";
    private final Connection connection;

    public SimpleQueryTagDao(ConnectionDataSource dataSource) {
        super(dataSource);
        connection = dataSource.getConnection();
    }

    @Override
    public void save(TagEntity value) {
        try {
            saveTag(value);
            connection.commit();
        } catch (SQLException e) {
            logger.error("Couldn't commit batch", e);
            throw new IllegalStateException();
        }
    }

    private void saveTag(TagEntity value) {
        try (Statement statement = connection.createStatement()) {
            String query = MessageFormat.format(INSERT_TAG_QUERY,
                    value.getNodeId().toString(),
                    value.getK().replaceAll("'", "''"),
                    value.getV().replaceAll("'", "''"));
            statement.execute(query);
        } catch (SQLException e) {
            logger.error("Couldn't insert node {}", value, e);
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
    }

}
