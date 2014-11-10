package com.trulia.thoth;

import com.trulia.thoth.message.QueueMessage;
import com.trulia.thoth.parser.Parser;
import com.trulia.thoth.pojo.RequestDocumentList;
import com.trulia.thoth.pojo.RequestDocument;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.util.ErrorHandler;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

/**
 * User: dbraga - Date: 7/19/14
 */

public class MessageImporter implements MessageListener, ErrorHandler {
  private static final Logger LOG = Logger.getLogger(MessageImporter.class);
  private static SolrServer server;
  private ThothConnection thothIndexConnection;
  private String configurationFile;
  private HashMap<String, Class> requestDocumentList;

  @Override
  public void handleError(Throwable throwable) {
    LOG.error(throwable);
  }

  @Override
  public void onMessage(Message message) {
    try{
      if (message instanceof TextMessage){
        String msgDequeued = ((TextMessage) message).getText();
        // Add information fetched from the message properties
        QueueMessage queueMessage = new QueueMessage(
                message.getStringProperty(QueueMessage.PORT),
                message.getStringProperty(QueueMessage.CORENAME),
                message.getStringProperty(QueueMessage.HOSTNAME),
                message.getStringProperty(QueueMessage.MESSAGE_TYPE),
                message.getStringProperty(QueueMessage.POOL_NAME)
        );
        Parser parser = new Parser(msgDequeued, queueMessage, requestDocumentList);

        if (parser.parsedCorrectly()) {
          server.add(parser.getSolrInputDocument());
          LOG.info("Added request for [" + message.getStringProperty(QueueMessage.HOSTNAME) +":"  + message.getStringProperty(QueueMessage.PORT ) + " core: " + message.getStringProperty(QueueMessage.CORENAME )+ "]");
        }
      }
    }
    catch (JMSException e){ LOG.error(e); }
    catch (IOException e){ LOG.error(e); }
    catch (SolrServerException e){ LOG.error(e); }
  }

  public void init() throws JAXBException, ClassNotFoundException {
    server = thothIndexConnection.getServer();

    requestDocumentList = new HashMap<String, Class>();
    // Reading monitor classes from configuration file
    JAXBContext jc = JAXBContext.newInstance(RequestDocumentList.class);
    Unmarshaller unmarshaller = jc.createUnmarshaller();
    RequestDocumentList obj = (RequestDocumentList)unmarshaller.unmarshal(new File(configurationFile));
    for (RequestDocument requestDocument : obj.getRequestDocuments()){
      System.out.println("Adding RequestDocument("+ requestDocument.getName()+") , className("+ requestDocument.getClassName()+") to available monitor list");
      requestDocumentList.put(requestDocument.getName(), Class.forName(requestDocument.getClassName()));
    }
  }

  public void setThothIndexConnection(ThothConnection thothIndexConnection) {
    this.thothIndexConnection = thothIndexConnection;
  }

  public ThothConnection getThothIndexConnection() {
    return thothIndexConnection;
  }

  public void setConfigurationFile(String configurationFile) {
    this.configurationFile = configurationFile;
  }

  public String getConfigurationFile() {
    return configurationFile;
  }
}
