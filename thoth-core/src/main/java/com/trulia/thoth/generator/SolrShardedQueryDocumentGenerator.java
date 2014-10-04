package com.trulia.thoth.generator;

import com.trulia.thoth.document.MessageDocument;
import com.trulia.thoth.document.SolrQueryDocument;
import com.trulia.thoth.document.SolrShardedQueryDocument;
import com.trulia.thoth.request.SolrShardedQueryRequest;

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
    document.setShardsInfo(solrShardedQueryRequest.getShardsInfo());
    document.setShardsParam(solrShardedQueryRequest.getShardParam());
    return document;
  }
}

