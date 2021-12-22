package protocols.agreement.notifications;

import protocols.agreement.model.OperationWrapper;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;

public class DecidedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 105;

    private final int instance;
    private final OperationWrapper operation;

    public DecidedNotification(int instance, OperationWrapper operation) {
        super(NOTIFICATION_ID);
        this.instance = instance;
        this.operation = operation;
    }

    public int getInstance() {
        return instance;
    }

    public OperationWrapper getOperation() {
        return operation;
    }

    @Override
    public String toString() {
        return "DecidedNotification{" +
                "instance=" + instance +
                ", operation=" + operation +
                '}';
    }
}
