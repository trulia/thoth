package com.trulia.thoth.generator;

import com.trulia.thoth.classifier.Classifier;
import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.document.SolrQueryDocument;
import com.trulia.thoth.document.SolrShardedQueryDocument;
import com.trulia.thoth.request.SolrShardedQueryRequest;
import com.trulia.thoth.util.QueryUtil;

/**
 * User: dbraga - Date: 3/2/14
 */
public class SolrShardedQueryDocumentGenerator implements Generator{

  SolrShardedQueryDocument document;
  SolrShardedQueryRequest solrShardedQueryRequest;

  public SolrShardedQueryDocumentGenerator(MessageDocument messageDocument, SolrShardedQueryRequest solrQueryRequest){
    this.document = new SolrShardedQueryDocument(new SolrQueryDocument(messageDocument));
    this.solrShardedQueryRequest = solrQueryRequest;
  }

  @Override
  public SolrShardedQueryDocument generate() {
    document.setDate(solrShardedQueryRequest.getTimestamp());
    document.setBitmask(new Classifier(solrShardedQueryRequest.getParams()).createBitMask());
    document.setParams(solrShardedQueryRequest.getParams());
    document.setHits(solrShardedQueryRequest.getHits());
    document.setStatus("0");  //TODO: remove hard coded status
    document.setQtime((int) solrShardedQueryRequest.getQtime());
    document.setSlowQueryTag(solrShardedQueryRequest.isSlowQueryTagPresent());
    document.setSlowQueryPredictionValidity(
        solrShardedQueryRequest.isSlowQueryTagPresent() == QueryUtil.isSlowQuery((int) solrShardedQueryRequest.getQtime())
    );
    document.setShardsInfo(solrShardedQueryRequest.getShardsInfo());
    document.setShardsParam(solrShardedQueryRequest.getShardParam());
    return document;
  }
}

