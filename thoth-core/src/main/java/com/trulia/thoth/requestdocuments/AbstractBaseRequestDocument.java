package com.trulia.thoth.requestdocuments;

import org.apache.solr.common.SolrInputDocument;

/**
 * Created by rshioramwar on 10/22/14.
 */
public abstract class AbstractBaseRequestDocument {

  public abstract void populateSolrInputDocument(SolrInputDocument solrInputDocument);
}
