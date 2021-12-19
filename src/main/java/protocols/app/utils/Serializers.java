package protocols.app.utils;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class Serializers {

    public static <T> ISerializer<T> nullable(ISerializer<T> serializer) {
        return new ISerializer<>() {
            @Override
            public void serialize(T obj, ByteBuf out) throws IOException {
                if (obj != null) {
                    out.writeBoolean(true);
                    serializer.serialize(obj, out);
                } else {
                    out.writeBoolean(false);
                }
            }

            @Override
            public T deserialize(ByteBuf in) throws IOException {
                if (in.readBoolean()) {
                    return serializer.deserialize(in);
                }
                return null;
            }
        };
    }

    public static ISerializer<UUID> uuid = new ISerializer<>() {
        @Override
        public void serialize(UUID uuid, ByteBuf out) {
            out.writeLong(uuid.getMostSignificantBits());
            out.writeLong(uuid.getLeastSignificantBits());
        }

        @Override
        public UUID deserialize(ByteBuf in) {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            return new UUID(firstLong, secondLong);
        }
    };

    public static ISerializer<String> string = new ISerializer<>() {
        @Override
        public void serialize(String string, ByteBuf out) {
            byte[] n = string.getBytes(StandardCharsets.UTF_8);
            out.writeInt(n.length);
            out.writeBytes(n);
        }

        @Override
        public String deserialize(ByteBuf in) {
            int stringLength = in.readInt();
            byte[] stringBytes = new byte[stringLength];
            in.readBytes(stringBytes);

            return new String(stringBytes, StandardCharsets.UTF_8);
        }
    };

    public static ISerializer<byte[]> byteArray = new ISerializer<>() {
        @Override
        public void serialize(byte[] byteArray, ByteBuf out) {
            out.writeInt(byteArray.length);
            if (byteArray.length > 0) {
                out.writeBytes(byteArray);
            }
        }

        @Override
        public byte[] deserialize(ByteBuf in) {
            int byteArrayLength = in.readInt();
            byte[] byteArray = new byte[byteArrayLength];
            if (byteArrayLength > 0)
                in.readBytes(byteArray);

            return byteArray;
        }
    };
}
