package software.amazon.glue.table;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import software.amazon.cloudformation.proxy.StdCallbackContext;

@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
public class CallbackContext extends StdCallbackContext {
    private boolean preExistenceCheckDone = false;
    private boolean preExistenceCheckDenied = false;
    private int limitedRetryCount = 0;
}
