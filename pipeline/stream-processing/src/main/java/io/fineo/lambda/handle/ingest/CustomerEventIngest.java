package io.fineo.lambda.handle.ingest;

import io.fineo.lambda.handle.LambdaResponseWrapper;
import io.fineo.lambda.handle.schema.org.ExternalOrgRequest;
import io.fineo.lambda.handle.schema.org.OrgHandler;
import io.fineo.lambda.handle.schema.response.OrgResponse;

/**
 *
 */
public class CustomerEventIngest extends
                                                          LambdaResponseWrapper<CustomerEventRequest,
                                                            CustomerEventResponse,
                                                            CustomerEventHandler> {
}
