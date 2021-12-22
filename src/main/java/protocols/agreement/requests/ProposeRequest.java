package protocols.agreement.requests;

import protocols.agreement.model.OperationWrapper;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Set;

public class ProposeRequest extends ProtoRequest {

    public static final short REQUEST_ID = 107;

    private final int instance;
    private final Set<Host> membership;
    private final OperationWrapper operation;

    public ProposeRequest(int instance, Set<Host> membership, OperationWrapper operation) {
        super(REQUEST_ID);
        this.instance = instance;
        this.membership = membership;
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
        return "ProposeRequest{" +
                "instance=" + instance +
                ", membership=" + membership +
                ", operation=" + operation +
                '}';
    }

    public Set<Host> getMembership() {
        return membership;
    }
}
