package protocols.agreement.model;

import io.netty.buffer.ByteBuf;
import protocols.util.Serializers;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
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

    public static OperationWrapper NULL = new OperationWrapper(null, OpType.NULL, null, null);

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
    public String toString() {
        return "OperationWrapper{" +
                "opId=" + opId +
                ", opType=" + opType +
                ", host=" + host +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(opId);
    }

    public Host getHost() {
        return host;
    }

    public static ISerializer<OperationWrapper> serializer = new ISerializer<>() {
        @Override
        public void serialize(OperationWrapper obj, ByteBuf out) throws IOException {
            Serializers.nullable(Serializers.uuid).serialize(obj.opId, out);
            out.writeInt(obj.opType.ordinal());
            Serializers.nullable(Serializers.byteArray).serialize(obj.operation, out);
            Serializers.nullable(Host.serializer).serialize(obj.host, out);
        }

        @Override
        public OperationWrapper deserialize(ByteBuf in) throws IOException {
            UUID uuid = Serializers.nullable(Serializers.uuid).deserialize(in);
            int opTypeOrdinal = in.readInt();
            OpType opType = OpType.values()[opTypeOrdinal];
            byte[] operation = Serializers.nullable(Serializers.byteArray).deserialize(in);
            Host host = Serializers.nullable(Host.serializer).deserialize(in);
            return new OperationWrapper(uuid, opType, operation, host);
        }
    };
}
