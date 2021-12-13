package protocols.statemachine.messages;

import java.util.UUID;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.agreement.messages.BroadcastMessage;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class JoinMessage extends ProtoMessage {
    public final static short MSG_ID = 211;

    //private final UUID opId;
    //private final int instance;
    //private final byte[] op;
    private UUID mid;

    public JoinMessage(UUID mid){
        super(MSG_ID);
        this.mid = mid;
        //this.instance = instance;
        //this.op = op;
        //this.opId = opId;
    }

    public UUID getMid() {
        return mid;
    }
    /**
    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }
*/
    @Override
    public String toString() {
        return "JoinMessage{" +
                "mid = " + this.mid +
                //"instance = " + this.instance +
                '}';
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<JoinMessage>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) {
            //out.writeInt(msg.instance);
            out.writeLong(msg.mid.getMostSignificantBits());
            out.writeLong(msg.mid.getLeastSignificantBits());
            //out.writeInt(msg.op.length);
            //out.writeBytes(msg.op);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) {
            //int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID mid = new UUID(highBytes, lowBytes);
            //byte[] op = new byte[in.readInt()];
            //in.readBytes(op);
            return new JoinMessage(mid);
        }
    };
}
