package com.trulia.thoth.shrinker;

/**
 * User: dbraga - Date: 11/7/14
 */
public class Configuration {
  public static final String NAME_RANGE_0_10 = "range-0-10_i";
  public static final String NAME_RANGE_10_100 = "range-10-100_i";
  public static final String NAME_RANGE_100_1000 = "range-100-1000_i";
  public static final String NAME_RANGE_1000_OVER = "range-1000-OVER_i";
  public static final String NAME_TOT_COUNT = "tot-count_i";
  public static final String ZERO_HITS_QUERIES_COUNT = "zeroHits-count_i";
  public static final String EXCEPTION_QUERIES_COUNT = "exceptionCount_i";
  public static final String RANGE_QUERIES_COUNT = "RangeQueryCount_i";
  public static final String FACET_QUERIES_COUNT = "FacetQueryCount_i";
  public static final String PROPERTY_LOOKUP_QUERIES_COUNT = "PropertyLookupQueryCount_i";
  public static final String PROPERTY_HASH_LOOKUP_QUERIES_COUNT = "PropertyHashLookupQueryCount_i";
  public static final String GEOSPATIAL_QUERIES_COUNT = "GeospatialQueryCount_i";
  public static final String OPENHOMES_QURIES_COUNT = "OpenHomesQueryCount_i";
  public static final String AVG_QTIME = "avg_qtime_d";
  public static final String AVG_REQUESTS_IN_PROGRESS = "avg_requestsInProgress_d";

  public static final String VALUE_RANGE_0_10 = "0 TO 10";
  public static final String VALUE_RANGE_10_100 = "11 TO 100";
  public static final String VALUE_RANGE_100_1000 = "101 TO 1000";
  public static final String VALUE_RANGE_1000_OVER = "1001 TO *";
  public static final String VALUE_TOT_COUNT = "* TO *";

  public static final String CONTAINS_RANGE_QUERY ="1??????";
  public static final String CONTAINS_FACET_QUERY ="?1?????";
  public static final String CONTAINS_PROPERTY_LOOKUP_QUERY ="??1????";
  public static final String CONTAINS_PROPERTY_HASH_LOOKUP_QUERY ="???1???";
  public static final String CONTAINS_GEOSPATIAL_QUERY ="?????1?";
  public static final String CONTAINS_OPEN_HOMES_QUERY ="??????1";
}
