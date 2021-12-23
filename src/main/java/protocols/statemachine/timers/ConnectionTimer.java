package protocols.statemachine.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class ConnectionTimer extends ProtoTimer {

    public static final short TIMER_ID = 200;

    private final Host host;

    public ConnectionTimer(Host host) {
        super(TIMER_ID);
        this.host = host;
    }

    @Override
    public ProtoTimer clone() {
        return new ConnectionTimer(host);
    }

    public Host getHost() {
        return host;
    }
}
