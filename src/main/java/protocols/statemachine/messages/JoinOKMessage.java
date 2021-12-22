package protocols.statemachine.messages;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.util.Serializers;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

public class JoinOKMessage extends ProtoMessage {
    public final static short MSG_ID = 202;

    private final UUID mid;
    private final byte[] state;
    private final int instance;
    private final Set<Host> membership;
    private final long sequenceNumber;

    public JoinOKMessage(UUID mid, int instance, byte[] state, Set<Host> membership, long sequenceNumber){
        super(MSG_ID);
        this.mid = mid;
        this.instance = instance;
        this.state = state;
        this.membership = membership;
        this.sequenceNumber = sequenceNumber;
    }

    public UUID getMid() {
        return mid;
    }

     public int getInstance() {
     return instance;
     }

    public byte[] getState() {
        return state;
    }

    public Set<Host> getMembership() {
        return membership;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "JoinOKMessage{" +
                "mid=" + mid +
                ", state=" + Arrays.toString(state) +
                ", instance=" + instance +
                ", membership=" + membership +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }

    public static ISerializer<JoinOKMessage> serializer = new ISerializer<JoinOKMessage>() {
        @Override
        public void serialize(JoinOKMessage msg, ByteBuf out) throws IOException {
            Serializers.uuid.serialize(msg.getMid(), out);
            Serializers.byteArray.serialize(msg.getState(), out);
            out.writeInt(msg.getInstance());
            Serializers.hashSet(Host.serializer).serialize(msg.getMembership(), out);
            out.writeLong(msg.getSequenceNumber());
        }

        @Override
        public JoinOKMessage deserialize(ByteBuf in) throws IOException {
            UUID mid = Serializers.uuid.deserialize(in);
            byte[] state = Serializers.byteArray.deserialize(in);
            int instance = in.readInt();
            Set<Host> membership = Serializers.hashSet(Host.serializer).deserialize(in);
            long sequenceNumber = in.readLong();
            return new JoinOKMessage(mid, instance, state, membership, sequenceNumber);
        }
    };
}
