package protocols.agreement.requests;

import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import org.apache.commons.codec.binary.Hex;

import java.util.UUID;

public class ProposeRequest extends ProtoRequest {

    public static final short REQUEST_ID = 101;

    private final int instance;
    private final UUID opId;
    //private final byte[] operation;
    private final Operation operation;

    public ProposeRequest(int instance, UUID opId, Operation operation) {
        super(REQUEST_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
    }

    public int getInstance() {
        return instance;
    }

    public Operation getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "ProposeRequest{" +
                "instance=" + instance +
                ", opId=" + opId +
                ", operation=" + operation +
                '}';
    }
}
