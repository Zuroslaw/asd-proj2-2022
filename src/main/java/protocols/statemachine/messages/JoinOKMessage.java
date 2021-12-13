package protocols.statemachine.messages;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.agreement.messages.BroadcastMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class JoinOKMessage extends ProtoMessage {
    public final static short MSG_ID = 212;

    //private final UUID opId;
    private final int instance;
    //private final byte[] op;
    private UUID mid;
    private byte[] state;

    public JoinOKMessage(UUID mid, int instance, byte[] state){
        super(MSG_ID);
        this.mid = mid;
        this.instance = instance;
        //this.op = op;
        //this.opId = opId;
        this.state = state;
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
/**
    public UUID getOpId() {
     return opId;
     }

     public byte[] getOp() {
     return op;
     }
     */
    @Override
    public String toString() {
        return "JoinOKMessage{" +
                "mid = " + this.mid +
                "instance = " + this.instance +
                "state included" +
                '}';
    }

    public static ISerializer<JoinOKMessage> serializer = new ISerializer<JoinOKMessage>() {
        @Override
        public void serialize(JoinOKMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());
            out.writeInt(msg.state.length);
            out.writeBytes(msg.state);
        }

        @Override
        public JoinOKMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID mid = new UUID(highBytes, lowBytes);
            byte[] state = new byte[in.readInt()];
            in.readBytes(state);
            return new JoinOKMessage(mid, instance, state);
        }
    };
}
