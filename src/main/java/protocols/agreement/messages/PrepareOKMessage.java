package protocols.agreement.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import protocols.agreement.model.OperationWrapper;
import protocols.app.utils.Operation;
import protocols.app.utils.Serializers;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareOKMessage extends ProtoMessage {

    public final static short MSG_ID = 101;

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
        return "BroadcastMessage{" +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<PrepareOKMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PrepareOKMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeLong(msg.sequenceNumber);
            out.writeLong(msg.highestAcceptedSeq);
            //todo: Serializers.byteArray.serialize(msg.highestAcceptedValue.toByteArray(), out);
        }

        @Override
        public PrepareOKMessage deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            long sequenceNumber = in.readLong();
            long highestAccept = in.readLong();
            //todo: byte[] operation = Serializers.byteArray.deserialize(in);
            return new PrepareOKMessage(instance, sequenceNumber, highestAccept, null /*todo*/);
        }
    };

}
