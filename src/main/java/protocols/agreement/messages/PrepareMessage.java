package protocols.agreement.messages;

import java.io.IOException;
import java.util.Calendar;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/*************************************************
 * This is here just as an example, your solution
 * probably needs to use different message types
 *************************************************/
public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

    private final UUID opId;
    private final int instance;
    private final Operation op;
    private long sequenceNumber;

    public PrepareMessage(int instance, UUID opId, Operation op) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.sequenceNumber = Calendar.getInstance().getTimeInMillis();
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public Operation getOp() {
        return op;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "BroadcastMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + op +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>(){
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) throws IOException{
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            //out.writeInt(msg.op.length);
            out.writeBytes(msg.op.toByteArray()); //had to throw IOException
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);

            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            Operation goodOp = Operation.fromByteArray(op); //had to throw IOException
            return new PrepareMessage(instance, opId, goodOp);
        }
    };

}
