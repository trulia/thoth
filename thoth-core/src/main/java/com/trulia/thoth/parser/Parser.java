package com.trulia.thoth.parser;


import com.trulia.thoth.mappers.Deserializer;
import com.trulia.thoth.message.QueueMessage;
import com.trulia.thoth.requestdocuments.AbstractBaseRequestDocument;
import com.trulia.thoth.requestdocuments.MessageRequestDocument;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.HashMap;

/**
 * User: dbraga - Date: 9/7/13
 */
public class Parser {
  private QueueMessage queueMessage;
  private ObjectMapper mapper;
  private MessageRequestDocument messageDocument;
  private String toParse;
  private SolrInputDocument solrInputDocument;

  private HashMap<String, Class> parserList;

  private String source;

  public MessageRequestDocument getMessageDocument(){
    return this.messageDocument;
  }

  public void generateMessageDocument() {
    messageDocument = new MessageRequestDocument();
    try{
      // Fetch information about the host contained in the queueMessage and copy them in the messageDocument
      messageDocument.setPortNumber(Integer.parseInt(queueMessage.getPort()));
      messageDocument.setHostname(queueMessage.getHostname());
      messageDocument.setCoreName(queueMessage.getCoreName());
      messageDocument.setMessageType(queueMessage.getMessageType());
      messageDocument.setPool(queueMessage.getPool());
      messageDocument.setSource(mapper.readValue(toParse, MessageRequestDocument.class).getSource());
    } catch (Exception ignored){}
  }

  public String getSource(){
    return this.messageDocument.getSource();
  }

  public Parser(String toParse, QueueMessage queueMessage, HashMap<String, Class> parserList) throws IOException {
    this.toParse = toParse;
    this.parserList = parserList;
    this.queueMessage = queueMessage;
    // Initialize the Object Mapper
    mapper = Deserializer.getInstance().getMapper();
    generateMessageDocument();
    source = getSource();
    if (source == null){   // Source was not set, skipping
      return;
    }
    generateSolrInputDocument();
  }

  public void generateSolrInputDocument() throws IOException{
    this.solrInputDocument = this.messageDocument.toSolrInputDocument();
    AbstractBaseRequestDocument req = null;

    if (parserList.get(source) != null){
      req = (AbstractBaseRequestDocument) mapper.readValue(toParse, parserList.get(source));
      req.populateSolrInputDocument(this.solrInputDocument);
    } else System.out.println("Source not recognized. Skipping...");
  }

  public SolrInputDocument getSolrInputDocument() {
    return solrInputDocument;
  }

  public boolean parsedCorrectly(){
    return solrInputDocument != null && getSource()!= null ;
  }
}
