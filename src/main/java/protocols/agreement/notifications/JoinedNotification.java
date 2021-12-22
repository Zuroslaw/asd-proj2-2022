package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;
import java.util.Set;

public class JoinedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 106;

    private final int joinInstance;
    private final long sequenceNumber;

    public JoinedNotification(int joinInstance, long sequenceNumber) {
        super(NOTIFICATION_ID);
        this.joinInstance = joinInstance;
        this.sequenceNumber = sequenceNumber;
    }

    public int getJoinInstance() {
        return joinInstance;
    }

    @Override
    public String toString() {
        return "JoinedNotification{" +
                ", joinInstance=" + joinInstance +
                '}';
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }
}
