package com.trulia.thoth.util;


import org.joda.time.DateTime;

import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * User: dbraga - Date: 10/12/13
 */
public class Utils {

  /**
   *
   * @param dateTime specific dateTime in joda datetime format.
   * @return YYYY-MM-DDThh:mm:ssZ in ZULU (UTC)format
   */
  public static String dateTimeToZuluSolrFormat(DateTime dateTime){
    final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SS";
    final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    sdf.setTimeZone(utc);
    return  sdf.format(dateTime.toDate()).toString()+"Z";
  }

  /**
   *
   * @param dateTime specific dateTime in joda datetime format.
   * @return YYYY-MM-DD hh:mm:ss in ZULU (UTC)format
   */
  public static String dateTimeToZuluFormat(DateTime dateTime){
    final String ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss:SSzzz";
    final SimpleDateFormat sdf = new SimpleDateFormat(ISO_FORMAT);
    final TimeZone utc = TimeZone.getTimeZone("UTC");
    sdf.setTimeZone(utc);
    return  sdf.format(dateTime.toDate());
  }



  /**
   * Creates a range query following Solr syntax
   * @param field field to be searched on
   * @param range desired range
   * @return
   */
  public static String createRangeQuery(String field, String range){
    return field + ":[" + range + "]";
  }

  /**
   * Creates a field/value query folowing Solr Syntax
   * @param field field to be searched on
   * @param value desired value
   * @return
   */
  public static String createFieldValueQuery(String field, String value){
    return field + ":" + value;
  }
}
