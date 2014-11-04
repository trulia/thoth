package com.trulia.thoth.requestdocuments;

import org.apache.solr.common.SolrInputDocument;

/**
 * Created by rshioramwar on 10/24/14.
 */
public class WatchingRequestDocument extends AbstractBaseRequestDocument {

  public static final String REQUESTS_IN_PROGRESS = "requestInProgress_i";
  public static final String BODY = "body_s";
  public static final String ERROR_REQUEST = "errorRequest_s";

  private String messageType;
  private String body;
  private int requestInProgress;
  private String errorRequest;

  public String getMessageType() {
    return messageType;
  }

  public void setMessageType(String messageType) {
    this.messageType = messageType;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  public int getRequestInProgress() {
    return requestInProgress;
  }

  public void setRequestInProgress(int requestIPprogress) {
    this.requestInProgress = requestIPprogress;
  }

  public String getErrorRequest() {
    return errorRequest;
  }

  public void setErrorRequest(String errorRequest) {
    this.errorRequest = errorRequest;
  }

  @Override
  public void populateSolrInputDocument(SolrInputDocument solrInputDocument) {

    if (getRequestInProgress() != -1) solrInputDocument.addField(REQUESTS_IN_PROGRESS, getRequestInProgress());
    if (!("").equals(getBody())) solrInputDocument.addField(BODY, getBody());
    if (!("").equals(getErrorRequest())) solrInputDocument.addField(ERROR_REQUEST, getErrorRequest());
  }
}
