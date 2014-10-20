package com.trulia.thoth.util;

import com.trulia.thoth.pojo.ServerDetail;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.apache.solr.client.solrj.response.PivotField;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;

import java.util.ArrayList;
import java.util.List;

/**
 * User: dbraga - Date: 10/4/14
 */
public class ThothServers {

  private static final String FACET_PIVOT_FIELDS = "hostname_s,coreName_s";
  private static final String POOL = "pool_s";
  private static final String PORT = "port_i";
  //TODO : externalize this
  private static final int FACET_LIMIT = 1000; // Bump this if you are monitoring more than 1K Servers



  private ServerDetail fetchServerDetails(String hostname, String coreName, SolrServer realTimeThoth) throws SolrServerException {
    System.out.println("Fetching server details for hostname("+hostname+") coreName("+coreName+")");
    QueryResponse qr = realTimeThoth.query(new SolrQuery("hostname_s:\"" + hostname + "\"" +" AND " + "coreName_s:\"" +coreName + "\" AND NOT exception_b:true AND NOT slowQuery_b:true").setRows(1));
    SolrDocumentList solrDocumentList = qr.getResults();
    ServerDetail sd = null;
    if (qr.getResults().getNumFound() > 0) {
      String pool = (String)solrDocumentList.get(0).getFieldValue(POOL);
      String port = solrDocumentList.get(0).getFieldValue(PORT).toString();
      sd = new ServerDetail(hostname, pool, port, coreName);
    }
    return sd;
  }

  public ArrayList<ServerDetail> getList(SolrServer realTimeThoth) throws SolrServerException {
    ArrayList<ServerDetail> serverDetails = new ArrayList<ServerDetail>();
    // Using HierarchicalFaceting to fetch server details .http://wiki.apache.org/solr/HierarchicalFaceting
    QueryResponse qr = realTimeThoth.query(new SolrQuery("*:*").addFacetPivotField(FACET_PIVOT_FIELDS).setRows(0).setFacetLimit(FACET_LIMIT));
    NamedList<List<PivotField>> pivots = qr.getFacetPivot();
    System.out.println("Found " + pivots.get(FACET_PIVOT_FIELDS).size()+" servers to monitor. Fetching information for these servers. Please wait");
    for (PivotField pivot: pivots.get(FACET_PIVOT_FIELDS)){
      String hostname = (String) pivot.getValue();
      for (PivotField pf: pivot.getPivot()){
        String coreName = (String) pf.getValue();
        ServerDetail detail = fetchServerDetails(hostname,coreName, realTimeThoth);
        if (detail != null) serverDetails.add(detail);
      }
    }
    return serverDetails;
  }

  public static void main(String[] args) throws SolrServerException {
    ThothServers thothServers = new ThothServers();
    ArrayList<ServerDetail> serverDetails = thothServers.getList(new HttpSolrServer("http://thoth.sv2.trulia.com:8983/solr/collection1"));
    System.out.println(serverDetails.size());
  }
}
