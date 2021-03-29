package ru.nsu.ccfit.khudyakov.db.dao.nodes;

import lombok.RequiredArgsConstructor;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.db.dao.Dao;
import ru.nsu.ccfit.khudyakov.model.Node;

@RequiredArgsConstructor
public abstract class NodeDao implements Dao<Node>, AutoCloseable {

    protected final ConnectionDataSource dataSource;

}
