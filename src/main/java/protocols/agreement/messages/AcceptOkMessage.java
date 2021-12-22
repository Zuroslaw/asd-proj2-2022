package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import protocols.agreement.model.OperationWrapper;
import protocols.util.Serializers;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AcceptOkMessage extends ProtoMessage {
    public final static short MSG_ID = 102;

    private final int instance;
    private final long sequenceNumber;
    private final OperationWrapper value;
    private final Set<Host> membership;

    public AcceptOkMessage(int instance, long sequenceNumber, OperationWrapper value, Set<Host> membership) {
        super(MSG_ID);
        this.instance = instance;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
        this.membership = membership;
    }

    public int getInstance() {
        return instance;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public OperationWrapper getValue() {
        return value;
    }

    public Set<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "AcceptOkMessage{" +
                "instance=" + instance +
                ", sequenceNumber=" + sequenceNumber +
                ", value=" + value +
                ", membership=" + membership +
                '}';
    }

    @SuppressWarnings("DuplicatedCode")
    public static ISerializer<AcceptOkMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AcceptOkMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.getInstance());
            out.writeLong(msg.getSequenceNumber());
            Serializers.nullable(OperationWrapper.serializer).serialize(msg.getValue(), out);
            out.writeInt(msg.getMembership().size());
            for (Host host : msg.getMembership()) {
                Host.serializer.serialize(host, out);
            }
        }

        @Override
        public AcceptOkMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            long sequenceNumber = in.readLong();
            OperationWrapper value = Serializers.nullable(OperationWrapper.serializer).deserialize(in);
            int membershipSize = in.readInt();
            Set<Host> membership = new HashSet<>();
            for (int i = 0; i < membershipSize; i++) {
                membership.add(Host.serializer.deserialize(in));
            }
            return new AcceptOkMessage(instance, sequenceNumber, value, membership);
        }
    };
}
