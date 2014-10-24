package com.trulia.thoth.parser;


import com.trulia.thoth.document.*;
import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.document.SolrExceptionDocument;
import com.trulia.thoth.document.SolrQueryDocument;
import com.trulia.thoth.document.SolrShardedQueryDocument;
import com.trulia.thoth.generator.SolrExceptionDocumentGenerator;
import com.trulia.thoth.generator.SolrQueryDocumentGenerator;
import com.trulia.thoth.generator.SolrShardedQueryDocumentGenerator;
import com.trulia.thoth.generator.WatchingDocumentGenerator;
import com.trulia.thoth.mappers.Deserializer;
import com.trulia.thoth.message.QueueMessage;
import com.trulia.thoth.request.*;
import com.trulia.thoth.requestdocuments.AbstractBaseRequestDocument;
import com.trulia.thoth.requestdocuments.SolrQueryRequestDocument;
import com.trulia.thoth.requestdocuments.SolrShardedQueryRequestDocument;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

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

  private String source;
  // Allowed sources
  private static final String WATCHING_HANDLER = "WatchingRequest";
  private static final String SOLR_QUERY = "SolrQuery";
  private static final String SOLR_SHARDED_QUERY = "SolrShardedQuery";
  private static final String SOLR_EXCEPTION = "ExceptionSolrQuery";


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

  public Parser(String toParse, QueueMessage queueMessage) throws IOException {
    this.toParse = toParse;
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
    if (source.equals(WATCHING_HANDLER)){
      generateWatchingRequest();
      WatchingDocument doc = new WatchingDocumentGenerator(getMessageDocument(), getWatchingRequest()).generate();
      solrInputDocument = doc.toSolrInputDocument();
    }
    if (source.equals(SOLR_QUERY)){
//      generateSolrQueryRequest();
//      SolrQueryDocument doc =  new SolrQueryDocumentGenerator(getMessageDocument(), getSolrQueryRequest()).generate();
//      solrInputDocument = doc.toSolrInputDocument();
      AbstractBaseRequestDocument req = mapper.readValue(toParse, SolrQueryRequestDocument.class);
      this.solrInputDocument = this.messageDocument.toSolrInputDocument();
      req.populateSolrInputDocument(this.solrInputDocument);

    }
    if (source.equals(SOLR_EXCEPTION)){
      generateSolrExceptionRequest();
      SolrExceptionDocument doc =  new SolrExceptionDocumentGenerator(getMessageDocument(), getSolrExceptionRequest()).generate();
      solrInputDocument = doc.toSolrInputDocument();
    }
    if (source.equals(SOLR_SHARDED_QUERY)){
//      generateSolrShardedQueryRequest();
//      SolrShardedQueryDocument doc = new SolrShardedQueryDocumentGenerator(getMessageDocument(), getSolrShardedQueryRequest()).generate();
//      solrInputDocument = doc.toSolrInputDocument();

      AbstractBaseRequestDocument req = mapper.readValue(toParse, SolrShardedQueryRequestDocument.class);
      this.solrInputDocument = this.messageDocument.toSolrInputDocument();
      req.populateSolrInputDocument(this.solrInputDocument);

    }
  }

  public SolrInputDocument getSolrInputDocument() {
    return solrInputDocument;
  }

  public boolean parsedCorreclty(){
    return solrInputDocument != null && getSource()!= null ;
  }
}
