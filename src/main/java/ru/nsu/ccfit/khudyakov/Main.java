package ru.nsu.ccfit.khudyakov;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.db.DbManager;
import ru.nsu.ccfit.khudyakov.db.dao.nodes.BatchQueryNodeDao;
import ru.nsu.ccfit.khudyakov.db.dao.nodes.NodeDao;
import ru.nsu.ccfit.khudyakov.db.dao.nodes.PreparedQueryNodeDao;
import ru.nsu.ccfit.khudyakov.db.dao.nodes.SimpleQueryNodeDao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.BatchQueryTagDao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.PreparedQueryTagDao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.SimpleQueryTagDao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.TagDao;
import ru.nsu.ccfit.khudyakov.input.InputContext;
import ru.nsu.ccfit.khudyakov.input.InputParser;
import ru.nsu.ccfit.khudyakov.services.NodeService;

public class Main {

    private static final Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        InputParser inputParser = new InputParser(args);
        InputContext inputContext = inputParser.parse();

        try (ConnectionDataSource dataSource = new ConnectionDataSource()) {
            DbManager dbManager = new DbManager(dataSource);
            dbManager.truncateNodeTable();

            logger.debug("Test simple query dao");
            testSimpleQuery(inputContext, dataSource);
            dbManager.truncateNodeTable();

            logger.debug("Test prepared query dao");
            testPreparedQuery(inputContext, dataSource);
            dbManager.truncateNodeTable();

            logger.debug("Test batch query dao");
            testBatchQuery(inputContext, dataSource);

        } catch (Exception e) {
            System.exit(1);
        }
    }

    private static void testSimpleQuery(InputContext inputContext, ConnectionDataSource dataSource) {
        try (NodeDao nodeDao = new SimpleQueryNodeDao(dataSource);
             TagDao tagDao = new SimpleQueryTagDao(dataSource)) {

            NodeService nodeService = new NodeService(nodeDao, tagDao);
            testPerformance(inputContext, nodeService);

        } catch (Exception e) {
            System.exit(1);
        }
    }

    private static void testPreparedQuery(InputContext inputContext, ConnectionDataSource dataSource) {
        try (NodeDao nodeDao = new PreparedQueryNodeDao(dataSource);
             TagDao tagDao = new PreparedQueryTagDao(dataSource)) {

            NodeService nodeService = new NodeService(nodeDao, tagDao);
            testPerformance(inputContext, nodeService);

        } catch (Exception e) {
            System.exit(1);
        }
    }

    private static void testBatchQuery(InputContext inputContext, ConnectionDataSource dataSource) {
        try (NodeDao nodeDao = new BatchQueryNodeDao(dataSource);
             TagDao tagDao = new BatchQueryTagDao(dataSource)) {

            NodeService nodeService = new NodeService(nodeDao, tagDao);
            testPerformance(inputContext, nodeService);

        } catch (Exception e) {
            System.exit(1);
        }
    }

    private static void testPerformance(InputContext inputContext, NodeService nodeService) {
        OsmProcessor osmProcessor = new OsmProcessor(nodeService);

        logger.debug("Start processing nodes");

        StopWatch watch = new StopWatch();
        watch.start();
        osmProcessor.saveNodes(inputContext);
        watch.stop();

        logger.debug("End processing nodes");
        logger.debug("Calcuation time: {}", watch);
        System.out.println("Calculation time: " + watch);
    }

}
