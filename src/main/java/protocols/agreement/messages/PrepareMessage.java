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

    private final int instance;
    private long sequenceNumber;

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
        return "BroadcastMessage{" +
                ", instance=" + instance +
                '}';
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>(){
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) throws IOException{
            out.writeInt(msg.instance);
            out.writeLong(msg.sequenceNumber);
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) throws IOException {
            long sequenceNumber = in.readLong();
            int instance = in.readInt();
            return new PrepareMessage(instance, sequenceNumber);
        }
    };

}
