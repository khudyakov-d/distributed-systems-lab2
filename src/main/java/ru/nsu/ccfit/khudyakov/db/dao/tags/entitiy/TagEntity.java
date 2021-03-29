package ru.nsu.ccfit.khudyakov.db.dao.tags.entitiy;

import lombok.Data;
import lombok.EqualsAndHashCode;
import ru.nsu.ccfit.khudyakov.model.Tag;

import java.math.BigInteger;

@Data
@EqualsAndHashCode(callSuper = true)
public class TagEntity extends Tag {

    private BigInteger nodeId;

    public TagEntity(BigInteger nodeId, String key, String value) {
        this.k = key;
        this.v = value;
        this.nodeId = nodeId;
    }

}
