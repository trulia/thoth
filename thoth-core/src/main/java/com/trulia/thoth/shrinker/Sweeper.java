package com.trulia.thoth.shrinker;



    import org.apache.log4j.Logger;
    import org.apache.solr.client.solrj.SolrServer;
    import org.apache.solr.client.solrj.SolrServerException;

    import java.io.IOException;

/**
 * User: dbraga - Date: 10/18/13
 */
public class Sweeper {
  private static final Logger LOG = Logger.getLogger(Sweeper.class);
  private SolrServer server;
  private String deleteQuery;


  public Sweeper(SolrServer server, String deleteQuery){
    this.server = server;
    this.deleteQuery = deleteQuery;
  }

  public void sweep() throws IOException, SolrServerException {
    LOG.info("Delete by query: " + deleteQuery);
    server.deleteByQuery(deleteQuery);
  }

}
