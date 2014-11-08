package com.trulia.thoth.shrinker;

import com.trulia.thoth.pojo.ServerDetail;
import com.trulia.thoth.util.ThothServers;
import com.trulia.thoth.util.Utils;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrServer;
import org.joda.time.DateTime;
import org.quartz.*;

import java.util.ArrayList;
import java.util.concurrent.*;

/**
 * User: dbraga - Date: 11/7/14
 */
public class ShrinkerJob implements Job {
  private static final Logger LOG = Logger.getLogger(ShrinkerJob.class);
  private static DateTime nowMinusTimeToShrink;
  private String thothIndexUrl;
  private int shrinkPeriod;
  private int threadPoolSize;
  private static final String realTimeCore = "collection1/";
  private static final String shrankCore = "shrank/";

  @Override
  public void execute(JobExecutionContext context) throws JobExecutionException {
    SchedulerContext schedulerContext = null;
    try {
      schedulerContext = context.getScheduler().getContext();
      shrinkPeriod = (Integer) schedulerContext.get("period");
      thothIndexUrl = (String) schedulerContext.get("thothIndexUrl");
      threadPoolSize = (Integer) schedulerContext.get("threadPoolSize");
      nowMinusTimeToShrink = new DateTime().minusSeconds(shrinkPeriod);
      LOG.info("Shrinking documents between " + Utils.dateTimeToZuluFormat(nowMinusTimeToShrink) + " and " + Utils.dateTimeToZuluFormat(new DateTime()));
      createDocumentShrinkers();
    } catch (SchedulerException e) {
      LOG.error(e);
    } catch (InterruptedException e) {
      LOG.error(e);
    } catch (ExecutionException e) {
      LOG.error(e);
    } catch (SolrServerException e) {
      LOG.error(e);
    }
  }

  public void createDocumentShrinkers() throws SolrServerException, InterruptedException, ExecutionException {
    ExecutorService service = Executors.newFixedThreadPool(threadPoolSize);
    CompletionService<String> completionService = new ExecutorCompletionService<String>(service);
    HttpSolrServer thothServer = new HttpSolrServer(thothIndexUrl + realTimeCore);
    HttpSolrServer thothShrankServer = new HttpSolrServer(thothIndexUrl + shrankCore);
    ArrayList<ServerDetail> listOfServers = new ThothServers().getList(thothServer);

    for (ServerDetail serverDetail: listOfServers){
      LOG.info("Shrinking docs for server("+serverDetail.getName()+"):("+serverDetail.getPort()+") ");
      completionService.submit(new DocumentShrinker(serverDetail, nowMinusTimeToShrink, thothServer, thothShrankServer));
    }

    // Wait for all the executors to finish
    for(int i = 0; i < listOfServers.size(); i++){
      completionService.take().get();
    }
    LOG.info("Done Shrinking.");
  }



}
