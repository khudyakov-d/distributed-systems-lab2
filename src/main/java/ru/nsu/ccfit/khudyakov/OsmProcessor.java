package ru.nsu.ccfit.khudyakov;


import lombok.RequiredArgsConstructor;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.nsu.ccfit.khudyakov.input.InputContext;
import ru.nsu.ccfit.khudyakov.model.Node;
import ru.nsu.ccfit.khudyakov.services.NodeService;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

@RequiredArgsConstructor
public class OsmProcessor {

    private static final Logger logger = LogManager.getLogger(OsmProcessor.class);

    private static final String NODE = "node";

    private static final String TAG = "tag";

    private static final int BATCH_THRESHOLD = 10000;

    private final NodeService nodeService;

    public void saveNodes(InputContext inputContext) {
        if (inputContext == null) {
            logger.error("Empty input");
            throw new IllegalStateException();
        }

        try (InputStream inputStream = new FileInputStream(inputContext.getArchiveFilePath());
             InputStream bufferedStream = new BufferedInputStream(inputStream);
             InputStream bzipStream = new BZip2CompressorInputStream(bufferedStream)) {

            saveNodes(bzipStream);

        } catch (IOException e) {
            logger.error("Error during opening archive file. " + e.getMessage(), e);
            throw new IllegalStateException();
        }
    }

    private void saveNodes(InputStream bzipStream) {
        try {
            Unmarshaller unmarshaller = getUnmarshaller();
            XMLStreamReader reader = getReader(bzipStream);

            List<Node> nodes = new ArrayList<>();

            while (reader.hasNext()) {
                reader.next();

                if (reader.isStartElement() && NODE.equals(reader.getLocalName())) {
                    Node node = (Node) unmarshaller.unmarshal(reader);
                    nodes.add(node);
                }

                if (nodes.size() > BATCH_THRESHOLD) {
                    nodeService.insertNodes(nodes);
                    nodes.clear();
                }
            }

            nodeService.insertNodes(nodes);

            return;
        } catch (JAXBException e) {
            logger.error("Couldn't instance jaxb factory", e);
        } catch (XMLStreamException e) {
            logger.error("Couldn't create xml stream reader", e);
        }

        throw new IllegalStateException();
    }

    private XMLStreamReader getReader(InputStream bzipStream) throws XMLStreamException {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        return new XsiTypeReader(factory.createXMLStreamReader(bzipStream));
    }

    private Unmarshaller getUnmarshaller() throws JAXBException {
        JAXBContext jaxbContext = JAXBContext.newInstance(Node.class);
        return jaxbContext.createUnmarshaller();
    }

    private static class XsiTypeReader extends StreamReaderDelegate {

        public XsiTypeReader(XMLStreamReader reader) {
            super(reader);
        }

        @Override
        public String getNamespaceURI() {
            if (getName().equals(new QName(NODE)) || getName().equals(new QName(TAG))) {
                return "http://openstreetmap.org/osm/0.6";
            }
            return super.getNamespaceURI();
        }

    }

}
