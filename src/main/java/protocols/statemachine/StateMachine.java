package protocols.statemachine;

import protocols.agreement.Paxos;
import protocols.agreement.model.OpType;
import protocols.agreement.model.OperationWrapper;
import protocols.agreement.notifications.JoinedNotification;
import protocols.app.HashApp;
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

    public List<Integer> getMyOpsExecuted() {
        return myOpsExecuted;
    }

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel

    private State state;
    private Set<Host> membership;
    private int currentInstance;
    private OperationWrapper currentOperation = null;

    private final Queue<OperationWrapper> queue = new LinkedList<>();

    public StateMachine(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        currentInstance = 1;

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
        registerMessageHandler(channelId, JoinMessage.MSG_ID, this::uponJoinMessage, this::uponMsgFail);
        registerMessageHandler(channelId, JoinOKMessage.MSG_ID, this::uponJoinOKMessage, this::uponMsgFail);

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

        int myIndex = initialMembership.indexOf(self);
        if (myIndex != -1) {
            state = State.ACTIVE;
            highestSequenceNumber = initialMembership.size();
            logger.info("Starting in ACTIVE as I am part of initial membership");
            logger.info("My sequence number: {}", myIndex);
            //I'm part of the initial membership, so I'm assuming the system is bootstrapping
            membership = new HashSet<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(0, myIndex));
        } else {
            state = State.JOINING;
            logger.info("Starting in JOINING as I am not part of initial membership");
            JoinMessage jMsg = new JoinMessage(UUID.randomUUID());
            Host target = initialMembership.get(0);
            sendMessage(jMsg, target, channelId);
            logger.info("{[]} JoinMessage sent from host {} to {}." + jMsg.getMid(), this.self, target);
        }

    }

    /*--------------------------------- Requests ---------------------------------------- */
    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        OperationWrapper operation = new OperationWrapper(request.getOpId(), OpType.CLIENT_REQUEST, request.getOperation(), null);
        if (state == State.JOINING) {
            logger.debug("I did not join yet, queueing the operation.");
            queue.offer(operation);
        } else if (state == State.ACTIVE) {
            if (currentOperation == null) {
                logger.debug("Proposing the order in current instance.");
                sendRequest(new ProposeRequest(currentInstance, membership, operation),
                        Paxos.PROTOCOL_ID);
                currentOperation = operation;
            } else {
                logger.debug("Queueing operation, because I'm already waiting for previous proposal to be decided.");
                queue.offer(operation);
            }
        }
    }

    private long highestSequenceNumber;

    private void uponCurrentStateReply(CurrentStateReply reply, short sourceProto){
        JoinOKMessage jOkMsg = new JoinOKMessage(UUID.randomUUID(), reply.getInstance(), reply.getState(), membership, ++highestSequenceNumber);
        sendMessage(jOkMsg, reply.getJoiner(), channelId);
    }

    List<DecidedNotification> toExecute = new LinkedList<>();

    private void handleAddReplica(DecidedNotification notification) {
        membership.add(notification.getOperation().getHost());
        if (hostsWaitingToJoin.contains(notification.getOperation().getHost())) {
            CurrentStateRequest request = new CurrentStateRequest(notification.getInstance(), notification.getOperation().getHost());
            sendRequest(request, HashApp.PROTO_ID);
        }
    }

    private void handleRemoveReplica(DecidedNotification notification) {
        membership.remove(notification.getOperation().getHost());
        if (notification.getOperation().getHost().equals(self)) {
            //todo: handle joining again
        }
    }

    private void execute(DecidedNotification notification) {
        OperationWrapper operation = notification.getOperation();
        switch (operation.getOpType()) {
            case NULL:
                break;
            case CLIENT_REQUEST:
                logger.debug("Client request operation decided. Sending execute back to App");
                triggerNotification(new ExecuteNotification(operation.getOpId(), operation.getOperation()));
                break;
            case ADD_REPLICA:
                handleAddReplica(notification);
                break;
            case REMOVE_REPLICA:
                handleRemoveReplica(notification);
        }
        currentInstance++;
    }

    private void executePending() { // todo refactor
        toExecute.sort(Comparator.comparing(DecidedNotification::getInstance));
        List<DecidedNotification> stillToExecute = new LinkedList<>();
        for (DecidedNotification notification : toExecute) {
            if (notification.getInstance() == currentInstance) {
                logger.debug("Executing pending operation from instance number {}", notification.getInstance());
                execute(notification);
            } else {
                stillToExecute.add(notification);
            }
        }
        toExecute = stillToExecute;
    }

    private List<Integer> myOpsExecuted = new LinkedList<>();

    /*--------------------------------- Notifications ---------------------------------------- */
    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);
        //Maybe we should make sure operations are executed in order?
        //You should be careful and check if this operation is an application operation (and send it up)
        //or if this is an operations that was executed by the state machine itself (in which case you should execute)
        if (notification.getInstance() > currentInstance) {
            logger.debug("This decided operation is not from my latest known instance, I will postpone the execution");
            toExecute.add(notification);
            return;
        }

        logger.debug("This decided operation is from current instance. Executing this operation and all pending operations (if any).");
        execute(notification);
        executePending();

        if (currentOperation == null) { // we don't execute anything, and we don't have any queued orders
            logger.debug("I didn't propose any operation, back to idle state");
            return;
        }

        if (Objects.equals(notification.getOperation().getOpId(), currentOperation.getOpId())) {
            logger.debug("Executed operation was the one proposed by me. Polling next operation from queue");
            myOpsExecuted.add(notification.getInstance());
            currentOperation = queue.poll();
            if (currentOperation != null) {
                logger.debug("Queue contains another operation. Proposing.");
                sendRequest(new ProposeRequest(currentInstance, membership, currentOperation),
                        Paxos.PROTOCOL_ID);
            }
        } else {
            logger.debug("Executed operation was NOT mine. Proposing my operation again.");
            sendRequest(new ProposeRequest(currentInstance, membership, currentOperation),
                    Paxos.PROTOCOL_ID);
        }
    }

    /*--------------------------------- Messages ---------------------------------------- */
    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    private final Set<Host> hostsWaitingToJoin = new HashSet<>();

    private void uponJoinMessage(JoinMessage msg, Host joiner, short sourceProtoId, int channelId) {
        hostsWaitingToJoin.add(joiner);
        OperationWrapper operation = new OperationWrapper(currentOperation.getOpId(), OpType.ADD_REPLICA, currentOperation.getOperation(), null);

        if (currentOperation == null) {
            sendRequest(new ProposeRequest(++currentInstance, membership, operation),
                    Paxos.PROTOCOL_ID);
        } else {
            queue.offer(operation);
        }
    }

    private void uponJoinOKMessage(JoinOKMessage msg, Host from, short sourceProtoId, int channelId){
        membership = msg.getMembership();
        highestSequenceNumber = msg.getSequenceNumber();
        currentInstance = msg.getInstance() + 1;
        InstallStateRequest insReq = new InstallStateRequest(msg.getState());
        sendRequest(insReq, HashApp.PROTO_ID);
        JoinedNotification joinedNotification = new JoinedNotification(currentInstance, highestSequenceNumber);
        triggerNotification(joinedNotification);
        state = State.ACTIVE;
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
        if(membership.contains(event.getNode())) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            openConnection(event.getNode());
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

}
