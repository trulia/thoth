package com.trulia.thoth.document;

import org.apache.solr.common.SolrInputDocument;

/**
 * User: dbraga - Date: 12/1/13
 */
public class SolrExceptionDocument extends SolrQueryDocument {
  private static String stackTrace;
  private static final String STACKTRACE = "stackTrace_s";
  public static final String EXCEPTION = "exception_b";

  public SolrInputDocument toSolrInputDocument(){
    SolrInputDocument solrInputDocument = super.toSolrInputDocument();
    if (stackTrace != null) solrInputDocument.addField(STACKTRACE, stackTrace);
    solrInputDocument.addField(EXCEPTION, true);
    return  solrInputDocument;
  }

  public SolrExceptionDocument(MessageDocument solrQueryDocument){
    super(solrQueryDocument);
  }

  public String getStackTrace() {
    return stackTrace;
  }

  public void setStackTrace(String stackTrace) { this.stackTrace = stackTrace; }


}
