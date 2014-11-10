package com.trulia.thoth.pojo;


    import javax.xml.bind.annotation.XmlAccessType;
    import javax.xml.bind.annotation.XmlAccessorType;
    import javax.xml.bind.annotation.XmlElement;
    import javax.xml.bind.annotation.XmlRootElement;
    import java.util.ArrayList;
    import java.util.List;

/**
 * User: dbraga - Date: 11/9/14
 */

@XmlAccessorType(XmlAccessType.FIELD)
@XmlRootElement(name = "RequestDocuments")
public class RequestDocumentList {
  @XmlElement(name="RequestDocument")
  private List<RequestDocument> requestDocuments = new ArrayList<RequestDocument>();

  public List<RequestDocument> getRequestDocuments() {
    return requestDocuments;
  }
  public void setRequestDocuments(List<RequestDocument> requestDocuments) {
    this.requestDocuments = requestDocuments;
  }
}