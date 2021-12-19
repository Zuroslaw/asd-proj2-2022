package protocols.agreement.model;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Objects;
import java.util.UUID;

public class OperationWrapper {
    private final UUID opId;
    private final OpType opType;
    private final byte[] operation; // in case its client request
    private final Host host; // in case its internal (add replica/remove replica)

    public OperationWrapper(UUID opId, OpType opType, byte[] operation, Host host) {
        this.opId = opId;
        this.opType = opType;
        this.operation = operation;
        this.host = host;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOperation() {
        return operation;
    }

    public OpType getOpType() {
        return opType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OperationWrapper that = (OperationWrapper) o;
        return Objects.equals(opId, that.opId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(opId);
    }

    public Host getHost() {
        return host;
    }
}
