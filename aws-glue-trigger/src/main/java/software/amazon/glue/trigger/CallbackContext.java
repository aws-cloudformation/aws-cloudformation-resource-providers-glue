package software.amazon.glue.trigger;

import software.amazon.awssdk.services.glue.model.GetTagsResponse;
import software.amazon.awssdk.services.glue.model.GetTriggerResponse;
import software.amazon.cloudformation.proxy.StdCallbackContext;

@lombok.Getter
@lombok.Setter
@lombok.ToString
@lombok.EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {

    GetTagsResponse getTagsResponse;
    GetTriggerResponse getTriggerResponse;

//    ResourceModel previousModel;

    boolean preExistenceCheckDone = false;
    boolean deletePreExistenceCheckDone = false;
}
