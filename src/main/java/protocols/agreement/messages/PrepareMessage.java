package protocols.agreement.messages;


import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

/*************************************************
 * This is here just as an example, your solution
 * probably needs to use different message types
 *************************************************/
public class PrepareMessage extends ProtoMessage {

    public final static short MSG_ID = 103;

    private final int instance;
    private final long sequenceNumber;

    public PrepareMessage(int instance, long sequenceNumber) {
        super(MSG_ID);
        this.instance = instance;
        this.sequenceNumber = sequenceNumber;
    }

    public int getInstance() {
        return instance;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    @Override
    public String toString() {
        return "PrepareMessage{" +
                "instance=" + instance +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<>(){
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.sequenceNumber);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long sequenceNumber = in.readLong();
            return new PrepareMessage(instance, sequenceNumber);
        }
    };

}
