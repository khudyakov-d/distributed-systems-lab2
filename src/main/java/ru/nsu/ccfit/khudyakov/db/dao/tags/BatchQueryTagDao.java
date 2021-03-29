package ru.nsu.ccfit.khudyakov.db.dao.tags;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.db.dao.tags.entitiy.TagEntity;

import java.sql.SQLException;
import java.util.List;

public class BatchQueryTagDao extends PreparedQueryTagDao {

    private static final Logger logger = LogManager.getLogger(BatchQueryTagDao.class);

    public BatchQueryTagDao(ConnectionDataSource dataSource) {
        super(dataSource);
    }

    @Override
    public void save(List<TagEntity> values) {
        try {
            for (TagEntity value : values) {
                insertStatement.setObject(1, value.getNodeId(), java.sql.Types.BIGINT);
                insertStatement.setString(2, value.getK());
                insertStatement.setString(3, value.getV());
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
