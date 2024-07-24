package software.amazon.glue.job;

import software.amazon.awssdk.services.glue.model.GetJobResponse;
import software.amazon.awssdk.services.glue.model.GetTagsResponse;
import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    // ResourceModel previousModel;
    boolean preExistenceCheckDone = false;
    boolean deletePreExistenceCheckDone = false;
    GetTagsResponse getTagsResponse;
    GetJobResponse getJobResponse;

}
