package protocols.agreement.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import protocols.agreement.model.OperationWrapper;
import protocols.util.Serializers;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareOKMessage extends ProtoMessage {

    public final static short MSG_ID = 104;

    private final int instance;
    private final long sequenceNumber;
    private final long highestAcceptedSeq;
    private final OperationWrapper highestAcceptedValue;

    public PrepareOKMessage(int instance, long sequenceNumber, long highestAcceptedSeq, OperationWrapper highestAcceptedValue) {
        super(MSG_ID);
        this.instance = instance;
        this.sequenceNumber = sequenceNumber;
        this.highestAcceptedSeq = highestAcceptedSeq;
        this.highestAcceptedValue = highestAcceptedValue;
    }

    public int getInstance() {
        return instance;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public long getHighestAcceptedSeq() {
        return highestAcceptedSeq;
    }

    public OperationWrapper getHighestAcceptedValue() {
        return highestAcceptedValue;
    }

    @Override
    public String toString() {
        return "PrepareOKMessage{" +
                "instance=" + instance +
                ", sequenceNumber=" + sequenceNumber +
                ", highestAcceptedSeq=" + highestAcceptedSeq +
                ", highestAcceptedValue=" + highestAcceptedValue +
                '}';
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeLong(msg.sequenceNumber);
            out.writeLong(msg.highestAcceptedSeq);
            Serializers.nullable(OperationWrapper.serializer).serialize(msg.getHighestAcceptedValue(), out);
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            long sequenceNumber = in.readLong();
            long highestAccept = in.readLong();
            OperationWrapper highestAcceptedValue = Serializers.nullable(OperationWrapper.serializer).deserialize(in);
            return new PrepareOKMessage(instance, sequenceNumber, highestAccept, highestAcceptedValue);
        }
    };

}
