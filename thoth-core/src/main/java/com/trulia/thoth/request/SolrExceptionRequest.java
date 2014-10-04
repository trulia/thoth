package com.trulia.thoth.request;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 * User: dbraga - Date: 11/23/13
 */
public class SolrExceptionRequest extends SolrQueryRequest{

  private String stackTrace;

  public String getStackTrace() {
    return stackTrace;
  }

  @JsonProperty("stackTrace")
  public void setStackTrace(String stackTrace) {
    this.stackTrace = stackTrace;
  }


}