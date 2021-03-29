package ru.nsu.ccfit.khudyakov.services;

import lombok.RequiredArgsConstructor;
import ru.nsu.ccfit.khudyakov.db.dao.nodes.NodeDao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.TagDao;
import ru.nsu.ccfit.khudyakov.db.dao.tags.entitiy.TagEntity;
import ru.nsu.ccfit.khudyakov.model.Node;

import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
public class NodeService {

    private final NodeDao nodeDao;

    private final TagDao tagDao;

    public void insertNodes(List<Node> nodes) {
        nodeDao.save(nodes);

        for (Node node : nodes) {
            List<TagEntity> tags = node.getTag().stream()
                    .map(t -> new TagEntity(node.getId(), t.getK(), t.getV()))
                    .collect(Collectors.toList());
            tagDao.save(tags);
        }

    }

}
