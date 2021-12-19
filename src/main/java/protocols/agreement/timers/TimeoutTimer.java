package protocols.agreement.timers;

import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class TimeoutTimer extends ProtoTimer {

    public static final short TIMER_ID = 301;

    private final int instance;

    public TimeoutTimer(int instance) {
        super(TIMER_ID);
        this.instance = instance;
    }

    @Override
    public ProtoTimer clone() {
        return new TimeoutTimer(this.instance);
    }

    public int getInstance() {
        return instance;
    }
}
