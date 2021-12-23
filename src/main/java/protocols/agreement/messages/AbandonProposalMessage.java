package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;


public class AbandonProposalMessage extends ProtoMessage {

    public final static short MSG_ID = 105;

    private final int instance;
    private final long sequenceNumber;

    public AbandonProposalMessage(int instance, long sequenceNumber) {
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
        return "AbandonProposalMessage{" +
                "instance=" + instance +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }

    public static ISerializer<AbandonProposalMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(AbandonProposalMessage msg, ByteBuf out){
            out.writeInt(msg.getInstance());
            out.writeLong(msg.getSequenceNumber());
        }

        @Override
        public AbandonProposalMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long sequenceNumber = in.readLong();
            return new AbandonProposalMessage(instance, sequenceNumber);
        }
    };
}
