package com.trulia.thoth.document;

import org.apache.solr.common.SolrInputDocument;

/**
 * User: dbraga - Date: 11/30/13
 */
public class WatchingDocument extends MessageDocument{

  public static final String REQUESTS_IN_PROGRESS = "requestInProgress_i";
  public static final String BODY = "body_s";
  public static final String ERROR_REQUEST = "errorRequest_s";


  private int requestsInprogress = -1;
  private String body = "";
  private String errorRequest = "";

  public WatchingDocument(MessageDocument messageDocument){
    this.coreName = messageDocument.getCoreName();
    this.portNumber = messageDocument.getPortNumber();
    this.hostname = messageDocument.getHostname();
    this.messageType = messageDocument.getMessageType();
    this.pool = messageDocument.getPool();
    this.source = messageDocument.getSource();
  }


  public void setRequestsInprogress(int requestsInprogress) {
    this.requestsInprogress = requestsInprogress;
  }

  public int getRequestsInprogress() {
    return requestsInprogress;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = (body == null ? "" : body) ;
  }

  public String getErrorRequest() {
    return errorRequest;

  }

  public void setErrorRequest(String errorRequest) {
    this.errorRequest = (errorRequest == null ? "" : errorRequest);
  }

  public  SolrInputDocument toSolrInputDocument(){

    SolrInputDocument solrInputDocument = super.toSolrInputDocument();
    if (getRequestsInprogress() != -1) solrInputDocument.addField(REQUESTS_IN_PROGRESS, getRequestsInprogress());
    if (!("").equals(getBody())) solrInputDocument.addField(BODY, getBody());
    if (!("").equals(getErrorRequest())) solrInputDocument.addField(ERROR_REQUEST, getErrorRequest());
    return  solrInputDocument;
  }

}
