package protocols.app.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;

public class CurrentStateRequest extends ProtoRequest {

    public static final short REQUEST_ID = 301;

    private int instance;
    private Host joiner;

    public CurrentStateRequest(int instance, Host joiner) {
        super(REQUEST_ID);
        this.instance = instance;
        this.joiner = joiner;
    }

    public int getInstance() {
    	return this.instance;
    }

    public Host getJoiner() {
        return joiner;
    }

    @Override
    public String toString() {
        return "CurrentStateRequest{" +
                "instance=" + instance +
                "joiner=" + joiner +
                '}';
    }
}
