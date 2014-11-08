package com.trulia.thoth.shrinker;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

/**
 * User: dbraga - Date: 11/7/14
 */
public class Scheduler {
  private String schedule;
  private boolean isEnabled;
  private int shrinkPeriod;
  private String thothIndexURL;

  public void setSchedule(String schedule) {
    this.schedule = schedule;
  }

  public String getSchedule() {
    return schedule;
  }

  public void init() throws SchedulerException {

    if (isEnabled){
      JobDetail workerJob = JobBuilder.newJob(ShrinkerJob.class)
          .withIdentity("pendingWorkJob", "group1").build();
      Trigger workerTrigger = TriggerBuilder
          .newTrigger()
          .withIdentity("pendingWorkTrigger", "group1")
          .withSchedule(
              CronScheduleBuilder.cronSchedule(schedule)) // execute this every day at midnight
          .build();

      //Schedule it
      org.quartz.Scheduler scheduler = new StdSchedulerFactory().getScheduler();
      scheduler.start();
      scheduler.getContext().put("period", shrinkPeriod);
      scheduler.getContext().put("thothIndexUrl", thothIndexURL);
      scheduler.scheduleJob(workerJob, workerTrigger);
    }
  }

  public void setIsEnabled(boolean isEnabled) {
    this.isEnabled = isEnabled;
  }

  public boolean getIsEnabled() {
    return isEnabled;
  }

  public void setShrinkPeriod(int shrinkPeriod) {
    this.shrinkPeriod = shrinkPeriod;
  }

  public int getShrinkPeriod() {
    return shrinkPeriod;
  }

  public void setThothIndexURL(String thothIndexURL) {
    this.thothIndexURL = thothIndexURL;
  }

  public String getThothIndexURL() {
    return thothIndexURL;
  }
}
