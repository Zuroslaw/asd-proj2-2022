package protocols.statemachine;

import protocols.agreement.notifications.JoinedNotification;
import protocols.app.HashApp;
import protocols.app.messages.RequestMessage;
import protocols.app.messages.ResponseMessage;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.JoinOKMessage;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.*;
import pt.unl.fct.di.novasys.network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.IncorrectAgreement;
import protocols.statemachine.notifications.ChannelReadyNotification;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.statemachine.notifications.ExecuteNotification;
import protocols.statemachine.requests.OrderRequest;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

/**
 * This is NOT fully functional StateMachine implementation.
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete anything in this class, including its fields.
 * The only thing that you cannot change are the notifications/requests between the StateMachine and the APPLICATION
 * You can change the requests/notification between the StateMachine and AGREEMENT protocol, however make sure it is
 * coherent with the specification shown in the project description.
 *
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class StateMachine extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;
    private List<Host> membership;
    private int nextInstance;

    private Queue<OrderRequest> queue = new LinkedList<>();

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        nextInstance = 0;

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port); //The port to bind to
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000");
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000");
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000");
        channelId = createChannel(TCPChannel.NAME, channelProps);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(OrderRequest.REQUEST_ID, this::uponOrderRequest);

        /*--------------------- Register Reply Handlers ----------------------------- */
        registerReplyHandler(CurrentStateReply.REQUEST_ID, this::uponCurrentStateReply);

        /*--------------------- Register Message Handlers ----------------------------- */
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoinMessage);
        registerMessageHandler(channelId, JoinOKMessage.MSG_ID, this::uponJoinOKMessage);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(DecidedNotification.NOTIFICATION_ID, this::uponDecidedNotification);

        /*--------------------- Register Message Serializers ----------------------------- */
        registerMessageSerializer(channelId, JoinMessage.MSG_ID, JoinMessage.serializer);
        registerMessageSerializer(channelId, JoinOKMessage.MSG_ID, JoinOKMessage.serializer);
    }

    @Override
    public void init(Properties props) {
        //Inform the state machine protocol about the channel we created in the constructor
        triggerNotification(new ChannelReadyNotification(channelId, self));

        String host = props.getProperty("initial_membership");
        String[] hosts = host.split(",");
        List<Host> initialMembership = new LinkedList<>();
        for (String s : hosts) {
            String[] hostElements = s.split(":");
            Host h;
            try {
                h = new Host(InetAddress.getByName(hostElements[0]), Integer.parseInt(hostElements[1]));
            } catch (UnknownHostException e) {
                throw new AssertionError("Error parsing initial_membership", e);
            }
            initialMembership.add(h);
        }

        if (initialMembership.contains(self)) {
            state = State.ACTIVE;
            logger.info("Starting in ACTIVE as I am part of initial membership");
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new LinkedList<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(membership, 0));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            //You have to do something to join the system and know which instance you joined
            // (and copy the state of that instance)
            JoinMessage jMsg = new JoinMessage(UUID.randomUUID());
            /**TODO: Add timeout so that he asks another one, OR ask everyone and stop listening to replies after the first on (if State==ACTIVE, that is already joined)?.
             * By now, we dont want too many messages flowing, so we assume the first host will be fine
            */
            Host target = initialMembership.get(0);
            sendMessage(jMsg, target, channelId);
            logger.info("{[]} JoinMessage sent from host {} to {}." + jMsg.getMid(), this.self, target);
        }

    }

    private final Set<UUID> internalOperationsIds = new HashSet<>();
    private OrderRequest currentOrder = null;

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        if (state == State.JOINING) {
            //DONE: Do something smart (like buffering the requests)
            queue.offer(request);
        } else if (state == State.ACTIVE) {

            if (currentOrder == null) {
                sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                        IncorrectAgreement.PROTOCOL_ID);
                currentOrder = request;
            } else {
                queue.offer(request);
            }

            //Also do something starter, we don't want an infinite number of instances active
        	//Maybe you should modify what is it that you are proposing so that you remember that this
        	//operation was issued by the application (and not an internal operation, check the uponDecidedNotification)
        }
    }

    private void uponCurrentStateReply(CurrentStateReply reply, short sourceProto){
        logger.info("{} CurrentStateReplyJoin in instance = {}, for joiner = {}, state included", self, reply.getInstance(), reply.getJoiner());

        JoinOKMessage jOkMsg = new JoinOKMessage(UUID.randomUUID(), reply.getInstance(), reply.getState());
        sendMessage(jOkMsg, reply.getJoiner(), channelId);

        JoinedNotification jNot = new JoinedNotification(this.membership, reply.getInstance());
        triggerNotification(jNot);
    }

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        //Maybe we should make sure operations are executed in order?
        //You should be careful and check if this operation is an application operation (and send it up)
        //or if this is an operations that was executed by the state machine itself (in which case you should execute)

        if (internalOperationsIds.contains(notification.getOpId())) {
            // do the internal operation //todo
        } else {
            triggerNotification(new ExecuteNotification(notification.getOpId(), notification.getOperation()));
            nextInstance = Math.max(notification.getInstance(), nextInstance);
            if (notification.getOpId().equals(currentOrder.getOpId())) {
                currentOrder = null;
            }
            if (!queue.isEmpty()) {
                OrderRequest request = queue.peek();
                currentOrder = request;
                sendRequest(new ProposeRequest(nextInstance++, request.getOpId(), request.getOperation()),
                        IncorrectAgreement.PROTOCOL_ID);
            }
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private void uponJoinMessage(JoinMessage msg, Host joiner, short sourceProtoId, int channelId) {
        logger.info("{[]} JoinMessage received from {}", msg.getMid(), joiner);
        membership.add(joiner);
        CurrentStateRequest csRequest = new CurrentStateRequest(this.nextInstance, joiner);
        sendRequest(csRequest, HashApp.PROTO_ID);
        logger.info("{} CurrentStateRequest for instance {}", self, this.nextInstance);
    }

    private void uponJoinOKMessage(JoinOKMessage msg, Host from, short sourceProtoId, int channelId){
        logger.info("{[]} JoinOKMessage received for joiner {}, joined on instance = {}, with state", msg.getMid(), self, msg.getInstance());
        if(!membership.contains(self))
            membership.add(self);
        state = State.ACTIVE;
        nextInstance = msg.getInstance();
        InstallStateRequest insReq = new InstallStateRequest(msg.getState());
        sendRequest(insReq, HashApp.PROTO_ID);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
    }

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.debug("Connection to {} is down, cause {}", event.getNode(), event.getCause());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());
        //Maybe we don't want to do this forever. At some point we assume he is no longer there.
        //Also, maybe wait a little bit before retrying, or else you'll be trying 1000s of times per second
        if(membership.contains(event.getNode()))
            openConnection(event.getNode());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
