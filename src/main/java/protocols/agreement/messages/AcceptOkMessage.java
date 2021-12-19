package protocols.agreement.messages;

import protocols.agreement.model.OperationWrapper;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;

public class AcceptOkMessage extends ProtoMessage {
    public final static short MSG_ID = 103;

    private final int instance;
    private final long sequenceNumber;
    private final OperationWrapper value;

    public AcceptOkMessage(int instance, long sequenceNumber, OperationWrapper value) {
        super(MSG_ID);
        this.instance = instance;
        this.sequenceNumber = sequenceNumber;
        this.value = value;
    }

    public int getInstance() {
        return instance;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public OperationWrapper getValue() {
        return value;
    }

    //todo serializers
}
