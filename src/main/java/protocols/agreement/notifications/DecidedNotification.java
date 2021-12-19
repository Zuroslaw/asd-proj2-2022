package protocols.agreement.notifications;

import java.util.UUID;

import org.apache.commons.codec.binary.Hex;

import protocols.agreement.model.OpType;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class DecidedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    private final int instance;
    private final UUID opId;
    private final byte[] operation;
    private final OpType opType;
    private final Host host; // in case its internal (add replica/remove replica)

    public DecidedNotification(int instance, UUID opId, OpType opType, byte[] operation, Host host) {
        super(NOTIFICATION_ID);
        this.instance = instance;
        this.opId = opId;
        this.operation = operation;
        this.opType = opType;
        this.host = host;
    }

    public int getInstance() {
        return instance;
    }

    public byte[] getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return opId;
    }

    @Override
    public String toString() {
        return "DecidedNotification{" +
                "instance=" + instance +
                ", opId=" + opId +
                ", operation=" + Hex.encodeHexString(operation) +
                '}';
    }

    public OpType getOpType() {
        return opType;
    }

    public Host getHost() {
        return host;
    }
}
