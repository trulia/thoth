package com.trulia.thoth.request;

/**
 * User: dbraga - Date: 11/28/13
 */
public class WatchingRequest {

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
}
