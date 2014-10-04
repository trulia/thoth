package com.trulia.thoth.generator;

import com.trulia.thoth.classifier.Classifier;
import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.document.SolrExceptionDocument;
import com.trulia.thoth.request.SolrExceptionRequest;

/**
 * User: dbraga - Date: 3/2/14
 */
public class SolrExceptionDocumentGenerator implements Generator{
  SolrExceptionDocument document;
  SolrExceptionRequest solrExceptionRequest;

  public SolrExceptionDocumentGenerator(MessageDocument messageDocument, SolrExceptionRequest solrExceptionRequest){
    this.document = new SolrExceptionDocument(messageDocument);
    this.solrExceptionRequest = solrExceptionRequest;

  }

  @Override
  public SolrExceptionDocument generate() {
    document.setDate(solrExceptionRequest.getTimestamp());
    document.setBitmask(new Classifier(solrExceptionRequest.getParams()).createBitMask());
    document.setParams(solrExceptionRequest.getParams());
    document.setHits(solrExceptionRequest.getHits());
    document.setStatus("0");  //TODO: remove hard coded status
    document.setQtime((int) solrExceptionRequest.getQtime());
    document.setStackTrace(solrExceptionRequest.getStackTrace());
    return document;
  }
}
