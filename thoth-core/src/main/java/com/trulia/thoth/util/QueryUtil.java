package com.trulia.thoth.util;

/**
 * User: dbraga - Date: 10/15/14
 */
public class QueryUtil {
  public static final int SLOW_QUERY_THRESHOLD = 100;

  /**
   * Identify if a query is slow / fast depending on qtime
   * @param qtime of the execution of the query
   * @return true or false depending if fast or slow
   */
  public static boolean isSlowQuery(int qtime){
    return (qtime > SLOW_QUERY_THRESHOLD); // ms
  }
}
