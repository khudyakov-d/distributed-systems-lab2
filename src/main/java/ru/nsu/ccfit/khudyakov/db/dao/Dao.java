package ru.nsu.ccfit.khudyakov.db.dao;

import java.util.List;

public interface Dao<T> {

    void save(T value);

    void save(List<T> values);

}
