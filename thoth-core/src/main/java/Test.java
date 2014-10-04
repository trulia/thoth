import com.trulia.thoth.pojo.QuerySamplingDetails;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * User: dbraga - Date: 8/11/14
 */
public class Test {

  public static void main(String[] args) throws IOException {
    String params="{\"details\":{\"rows\":30,\"start\":0,\"query\":\"(propertyId_s:\\\"1057517954\\\")\",\"slowpool\":false},\"features\":{\"containsOpenHomes\":false,\"facet\":false,\"collapsingSearch\":false,\"geospatialSearch\":false,\"rangeQuery\":false}}";
    ObjectMapper mapper = new ObjectMapper();
    QuerySamplingDetails querySamplingDetails = mapper.readValue(params, QuerySamplingDetails.class);
    QuerySamplingDetails.Details details = querySamplingDetails.getDetails();

  }
}
