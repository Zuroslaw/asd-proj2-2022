package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.UUID;

public class AddReplicaRequest extends ProtoRequest {

    public static final short REQUEST_ID = 103;

    private final int instance;
    private final UUID opId;
    private final Host replica;

    public AddReplicaRequest(int instance, UUID opId, Host replica) {
        super(REQUEST_ID);
        this.instance = instance;
        this.opId = opId;
        this.replica = replica;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public Host getReplica() {
    	return replica;
    }
   

    @Override
    public String toString() {
        return "ProposeRequest{" +
                "instance=" + instance +
                ", replica=" + replica +
                '}';
    }
}
