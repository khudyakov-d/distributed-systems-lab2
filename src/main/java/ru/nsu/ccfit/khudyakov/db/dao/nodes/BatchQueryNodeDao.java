package ru.nsu.ccfit.khudyakov.db.dao.nodes;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.model.Node;

import java.sql.SQLException;
import java.util.List;

public class BatchQueryNodeDao extends PreparedQueryNodeDao {

    private static final Logger logger = LogManager.getLogger(PreparedQueryNodeDao.class);

    public BatchQueryNodeDao(ConnectionDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public void save(List<Node> values) {
        try {
            for (Node value : values) {
                insertStatement.setObject(1, value.getId(), java.sql.Types.BIGINT);
                insertStatement.setString(2, value.getUser());
                insertStatement.setDouble(3, value.getLon());
                insertStatement.setDouble(4, value.getLat());
                insertStatement.addBatch();
            }

            insertStatement.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            logger.error("Error during batch inserting", e);
            throw new IllegalStateException();
        }
    }

}
