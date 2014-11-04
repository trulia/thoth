package com.trulia.thoth.requestdocuments;

import com.trulia.thoth.document.MessageDocument;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Created by rshioramwar on 10/24/14.
 */
public class SolrExceptionRequestDocument extends SolrQueryRequestDocument {

  private String stackTrace;

  private static final String STACKTRACE = "stackTrace_s";
  public static final String EXCEPTION = "exception_b";

  public String getStackTrace() {
    return stackTrace;
  }

  @JsonProperty("stackTrace")
  public void setStackTrace(String stackTrace) {
    this.stackTrace = stackTrace;
  }

  public void populateSolrInputDocument(SolrInputDocument solrInputDocument){
    super.populateSolrInputDocument(solrInputDocument);
    if (stackTrace != null) solrInputDocument.addField(STACKTRACE, stackTrace);
    solrInputDocument.addField(EXCEPTION, true);

  }
}
