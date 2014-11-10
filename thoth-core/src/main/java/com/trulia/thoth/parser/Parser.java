package com.trulia.thoth.parser;


import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.mappers.Deserializer;
import com.trulia.thoth.message.QueueMessage;
import com.trulia.thoth.request.*;
import com.trulia.thoth.requestdocuments.*;
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
  private MessageDocument messageDocument;
  private String toParse;
  private SolrInputDocument solrInputDocument;


  private WatchingRequest watchingRequest;
  private SolrQueryRequest solrQueryRequest;
  private SolrShardedQueryRequest solrShardedQueryRequest;
  private SolrExceptionRequest solrExceptionRequest;
  private HashMap<String, Class> parserList;

  private String source;

  public MessageDocument getMessageDocument(){
    return this.messageDocument;
  }

  public void generateWatchingRequest() throws IOException {
    this.watchingRequest = mapper.readValue(toParse, WatchingRequest.class);
  }

  public void generateSolrExceptionRequest() throws IOException {
    this.solrExceptionRequest = mapper.readValue(toParse, SolrExceptionRequest.class);
  }


  public void generateSolrQueryRequest() throws IOException {
    this.solrQueryRequest = mapper.readValue(toParse, SolrQueryRequest.class);
  }

  public SolrShardedQueryRequest getSolrShardedQueryRequest() {
    return solrShardedQueryRequest;
  }

  public void generateSolrShardedQueryRequest() throws IOException {
    this.solrShardedQueryRequest = mapper.readValue(toParse, SolrShardedQueryRequest.class);
    this.solrShardedQueryRequest.setShardParam();
  }

  public WatchingRequest getWatchingRequest() {
    return this.watchingRequest;
  }

  public SolrQueryRequest getSolrQueryRequest(){
    return this.solrQueryRequest;
  }

  public SolrExceptionRequest getSolrExceptionRequest(){
    return this.solrExceptionRequest;
  }

  public void generateMessageDocument() {
    messageDocument = new MessageDocument();
    try{
      // Fetch information about the host contained in the queueMessage and copy them in the messageDocument
      messageDocument.setPortNumber(Integer.parseInt(queueMessage.getPort()));
      messageDocument.setHostname(queueMessage.getHostname());
      messageDocument.setCoreName(queueMessage.getCoreName());
      messageDocument.setMessageType(queueMessage.getMessageType());
      messageDocument.setPool(queueMessage.getPool());
      messageDocument.setSource(mapper.readValue(toParse, Message.class).getSource());
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
