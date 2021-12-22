package protocols.statemachine.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import protocols.util.Serializers;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class JoinMessage extends ProtoMessage {
    public final static short MSG_ID = 201;

    private final UUID mid;

    public JoinMessage(UUID mid){
        super(MSG_ID);
        this.mid = mid;
    }

    public UUID getMid() {
        return mid;
    }

    @Override
    public String toString() {
        return "JoinMessage{" +
                "mid=" + mid +
                '}';
    }

    public static ISerializer<JoinMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(JoinMessage msg, ByteBuf out) throws IOException {
            Serializers.uuid.serialize(msg.getMid(), out);
        }

        @Override
        public JoinMessage deserialize(ByteBuf in) throws IOException {
            UUID mid = Serializers.uuid.deserialize(in);
            return new JoinMessage(mid);
        }
    };
}
