package ru.nsu.ccfit.khudyakov.db.dao.tags;

import lombok.RequiredArgsConstructor;
import ru.nsu.ccfit.khudyakov.db.ConnectionDataSource;
import ru.nsu.ccfit.khudyakov.db.dao.Dao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.entitiy.TagEntity;

@RequiredArgsConstructor
public abstract class TagDao implements Dao<TagEntity>, AutoCloseable {

    protected final ConnectionDataSource dataSource;

}
