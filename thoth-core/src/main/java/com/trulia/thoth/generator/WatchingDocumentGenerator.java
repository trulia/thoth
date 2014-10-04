package com.trulia.thoth.generator;


import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.document.WatchingDocument;
import com.trulia.thoth.request.WatchingRequest;

/**
 * User: dbraga - Date: 3/2/14
 */
public class WatchingDocumentGenerator implements Generator {
  WatchingDocument document;
  WatchingRequest watchingRequest;

  public WatchingDocumentGenerator(MessageDocument messageDocument, WatchingRequest watchingRequest){
    this.document = new WatchingDocument(messageDocument);
    this.watchingRequest = watchingRequest;

  }

  @Override
  public WatchingDocument generate() {
    document.setRequestsInprogress(watchingRequest.getRequestInProgress());
    document.setBody(watchingRequest.getBody());
    document.setErrorRequest(watchingRequest.getErrorRequest());
    return document;
  }

  public WatchingDocument getDocument() {
    return document;
  }
}
