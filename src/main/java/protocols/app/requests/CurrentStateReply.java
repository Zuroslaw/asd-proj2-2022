package protocols.app.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoReply;
import pt.unl.fct.di.novasys.network.data.Host;

public class CurrentStateReply extends ProtoReply {

    public static final short REQUEST_ID = 301;

    private int instance;
    private byte[] state;
    private Host joiner;

    public CurrentStateReply(int instance, byte[] state, Host joiner) {
        super(REQUEST_ID);
        this.instance = instance;
        this.state = state;
        this.joiner = joiner;
    }

    public int getInstance() {
    	return this.instance;
    }
    
    public byte[] getState() {
    	return this.state;
    }

    public Host getJoiner() {
        return joiner;
    }

    @Override
    public String toString() {
        return "CurrentStateReply{" +
                "instance=" + instance +
                "number of bytes=" + state.length +
                "joiner=" + joiner +
                '}';
    }
}
