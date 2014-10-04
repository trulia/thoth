package com.trulia.thoth;

import com.trulia.thoth.message.QueueMessage;
import com.trulia.thoth.parser.Parser;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.springframework.util.ErrorHandler;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.io.IOException;

/**
 * User: dbraga - Date: 7/19/14
 */
public class MessageImporter implements MessageListener, ErrorHandler {
  private static final Logger LOG = Logger.getLogger(MessageImporter.class);
  private static SolrServer server;
  private ThothConnection thothIndexConnection;

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
        Parser parser = new Parser(msgDequeued, queueMessage);

        if (parser.parsedCorreclty()) {
          server.add(parser.getSolrInputDocument());
          LOG.info("Added query for [" + message.getStringProperty(QueueMessage.HOSTNAME) +":"  + message.getStringProperty(QueueMessage.PORT ) + " core: " + message.getStringProperty(QueueMessage.CORENAME )+ "]");
        }
      }
    }
    catch (JMSException e){ LOG.error(e); }
    catch (IOException e){ LOG.error(e); }
    catch (SolrServerException e){ LOG.error(e); }
  }

  public void init() {
    server = thothIndexConnection.getServer();
  }

  public void setThothIndexConnection(ThothConnection thothIndexConnection) {
    this.thothIndexConnection = thothIndexConnection;
  }

  public ThothConnection getThothIndexConnection() {
    return thothIndexConnection;
  }
}
