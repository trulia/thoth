package com.trulia.thoth.generator;


import com.trulia.thoth.classifier.Classifier;
import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.document.SolrQueryDocument;
import com.trulia.thoth.request.SolrQueryRequest;

/**
 * User: dbraga - Date: 3/2/14
 */
public class SolrQueryDocumentGenerator implements Generator{
  SolrQueryDocument document;
  SolrQueryRequest solrQueryRequest;

  public SolrQueryDocumentGenerator(MessageDocument messageDocument, SolrQueryRequest solrQueryRequest){
    this.document = new SolrQueryDocument(messageDocument);
    this.solrQueryRequest = solrQueryRequest;
  }

  @Override
  public SolrQueryDocument generate() {
    document.setDate(solrQueryRequest.getTimestamp());
    document.setBitmask(new Classifier(solrQueryRequest.getParams()).createBitMask());
    document.setParams(solrQueryRequest.getParams());
    document.setHits(solrQueryRequest.getHits());
    document.setStatus("0");  //TODO: remove hard coded status
    document.setQtime((int) solrQueryRequest.getQtime());
    return document;
  }

}
