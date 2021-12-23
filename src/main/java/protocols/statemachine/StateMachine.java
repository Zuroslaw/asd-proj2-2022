package protocols.statemachine;

import protocols.agreement.model.OpType;
import protocols.agreement.model.OperationWrapper;
import protocols.agreement.notifications.JoinedNotification;
import protocols.app.HashApp;
import protocols.app.requests.CurrentStateReply;
import protocols.app.requests.CurrentStateRequest;
import protocols.app.requests.InstallStateRequest;
import protocols.statemachine.messages.JoinMessage;
import protocols.statemachine.messages.JoinOKMessage;
import protocols.statemachine.timers.ConnectionTimer;
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


public class StateMachine extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(StateMachine.class);

    private enum State {JOINING, ACTIVE}

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "StateMachine";
    public static final short PROTOCOL_ID = 200;

    private final Host self;     //My own address/port
    private final int channelId; //Id of the created channel
    private final short AGREEMENT_PROTO_ID;

    /* Operations/proposals */
    private State state;
    private int currentInstance;
    private OperationWrapper currentOperation = null; // null indicates that we did not propose anything in current instance
    private final Queue<OperationWrapper> queue = new LinkedList<>(); // queue of orders that need to be proposed, but are waiting for previous order to complete
    private List<DecidedNotification> toExecute = new LinkedList<>(); // notifications that has been decided, but I can't execute yet because it's a future instance

    /* membership */
    private Set<Host> membership;
    private long highestSequenceNumber; // highest sequence number assigned to replicas, it is sent to newly joining replicas
    private final Set<Host> hostsWaitingToJoin = new HashSet<>(); // hosts that contacted me to join, I keep track of them to know to send them the state

    /* Failed connections handling */
    private final Map<Host, Integer> connectionsPending = new HashMap<>(); // Used to keep track of reconnect attempts for hosts that I lost connection with
    private Set<Host> initialMembershipConnectionsPending = new HashSet<>(); // that's just to make sure that at the beginning we wait indefinitely for nodes
    private final int maxReconnectAttempts;
    private final long waitTimeBetweenReconnects;

    public StateMachine(Properties props, short agreementProtoId) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        AGREEMENT_PROTO_ID = agreementProtoId;
        currentInstance = 1;

        String address = props.getProperty("address");
        String port = props.getProperty("p2p_port");

        logger.info("Listening on {}:{}", address, port);
        this.self = new Host(InetAddress.getByName(address), Integer.parseInt(port));

        maxReconnectAttempts = Integer.parseInt(props.getProperty("sm_max_reconnect_attempts", "15"));
        waitTimeBetweenReconnects = Long.parseLong(props.getProperty("sm_wait_time_between_reconnects_ms", "1000"));

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

        registerTimerHandler(ConnectionTimer.TIMER_ID, this::uponConnectionTimer);
    }

    @Override
    public void init(Properties props) {
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

        int mySequenceNumber = initialMembership.indexOf(self);
        if (mySequenceNumber != -1) {
            state = State.ACTIVE;
            highestSequenceNumber = initialMembership.size() - 1;
            logger.info("Starting in ACTIVE as I am part of initial membership. My sequence number: {}", mySequenceNumber);
            membership = new HashSet<>(initialMembership);
            initialMembershipConnectionsPending = new HashSet<>(initialMembership);
            membership.forEach(this::openConnection);
            triggerNotification(new JoinedNotification(0, mySequenceNumber));
        } else {
            logger.info("Starting in JOINING as I am not part of initial membership");
            beginJoin(initialMembership.get(0));
        }

    }

    /*--------------------------------- ORDERS / PROPOSALS ---------------------------------------- */

    // procedure to propose operation if nothing pending or enqueuing it if I already proposed something
    private void proposeOrEnqueue(OperationWrapper operation) {
        if (currentOperation == null) {
            logger.debug("Proposing operation: {}", operation);
            currentOperation = operation;
            sendRequest(new ProposeRequest(currentInstance, membership, operation),
                    AGREEMENT_PROTO_ID);
        } else {
            logger.debug("I already have a pending proposed operation. Putting operation to queue: {}", operation);
            queue.offer(operation);
        }
    }

    private void uponOrderRequest(OrderRequest request, short sourceProto) {
        logger.debug("Received request: " + request);
        OperationWrapper operation = new OperationWrapper(request.getOpId(), OpType.CLIENT_REQUEST, request.getOperation(), null);
        if (state == State.ACTIVE) {
            proposeOrEnqueue(operation);
        } else {
            logger.debug("I did not join yet, queueing the operation.");
            queue.offer(operation);
        }
    }

    private void uponDecidedNotification(DecidedNotification notification, short sourceProto) {
        logger.debug("Received notification: " + notification);

        if (notification.getInstance() > currentInstance) {
            logger.debug("This decided operation is not from my latest known instance, I will postpone the execution");
            toExecute.add(notification);
            if (currentOperation == null) {
                /*
                    This part is to ensure that a replica with no client requests will still progress with learning the decision
                    if there were messages lost and the replica can't learn a decision. It is useful especially for newly joined replicas
                    which can lose some messages sent to them when they are still in JOINING state (and they do not receive client requests).
                    Maybe it could be done on Paxos level, by queuing the messages, and maybe this solution is not useful in real world (replicas could just
                    at some point after startup send a read request to catch up), but that's what useful for us to test if joining behaves correctly.
                    Also, maybe a better idea in real world situation would be to use a timer here that checks periodically if replica is lagging behind,
                    instead of doing it here.
                 */
                logger.debug("I'm lagging behind with my execution and I don't have any pending orders. I need to propose NULL to get previous decisions");
                currentOperation = OperationWrapper.NULL;
                sendRequest(new ProposeRequest(currentInstance, membership, currentOperation),
                        AGREEMENT_PROTO_ID);
            }
            return;
        }

        logger.debug("This decided operation is from current instance. Executing this operation and all pending operations (if any).");
        execute(notification);
        executePending();

        if (currentOperation == null) { // if we didn't propose anything, then queue must be empty
            logger.debug("I didn't propose any operation, back to idle state");
            return;
        }

        if (Objects.equals(notification.getOperation().getOpId(), currentOperation.getOpId())) {
            logger.debug("Executed operation was the one proposed by me. Polling next operation from queue");
            currentOperation = queue.poll();
            if (currentOperation != null) {
                logger.debug("Queue contains another operation. Proposing: {}", currentOperation);
                sendRequest(new ProposeRequest(currentInstance, membership, currentOperation),
                        AGREEMENT_PROTO_ID);
            } else {
                logger.debug("Queue empty. Going idle.");
            }
        } else {
            logger.debug("Executed operation was NOT mine. Proposing my operation again.");
            sendRequest(new ProposeRequest(currentInstance, membership, currentOperation),
                    AGREEMENT_PROTO_ID);
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

    private void executePending() {
        toExecute.sort(Comparator.comparing(DecidedNotification::getInstance)); // need to make sure they are in order
        List<DecidedNotification> stillToExecute = new LinkedList<>();
        for (DecidedNotification notification : toExecute) {
            if (notification.getInstance() == currentInstance) { // we want to execute pending operations without gaps
                logger.debug("Executing pending operation from instance number {}", notification.getInstance());
                execute(notification);
            } else {
                stillToExecute.add(notification);
            }
        }
        toExecute = stillToExecute;
    }

    private void handleAddReplica(DecidedNotification notification) {
        membership.add(notification.getOperation().getHost());
        highestSequenceNumber++;
        openConnection(notification.getOperation().getHost(), channelId);
        logger.info("ADD_REPLICA decided, Host {} added to membership", notification.getOperation().getHost());
        if (hostsWaitingToJoin.contains(notification.getOperation().getHost())) {
            // The host who contact me has been added to membership. Now I need to get the state of this instance and send it later to the host.
            hostsWaitingToJoin.remove(notification.getOperation().getHost());
            CurrentStateRequest request = new CurrentStateRequest(notification.getInstance(), notification.getOperation().getHost());
            sendRequest(request, HashApp.PROTO_ID);
        }
    }

    private void uponCurrentStateReply(CurrentStateReply reply, short sourceProto){
        // Got state for given instance. Sending it to the newly joined host.
        JoinOKMessage jOkMsg = new JoinOKMessage(UUID.randomUUID(), reply.getInstance(), reply.getState(), membership, highestSequenceNumber);
        sendMessage(jOkMsg, reply.getJoiner());
    }

    private void handleRemoveReplica(DecidedNotification notification) {
        membership.remove(notification.getOperation().getHost());
        logger.info("REMOVE_REPLICA decided, Host {} removed from membership", notification.getOperation().getHost());
        if (notification.getOperation().getHost().equals(self)) {
            logger.info("Trying to Join again, as I was removed from membership");
            beginJoin(membership.stream().findFirst().orElseThrow());
        }
    }

    /* ------------------------- JOINING ---------------------- */

    private void beginJoin(Host contactNode) { // used at startup or after being removed from membership
        state = State.JOINING;
        openConnection(contactNode);
        JoinMessage jMsg = new JoinMessage(UUID.randomUUID());
        sendMessage(jMsg, contactNode);
        logger.info("JoinMessage sent from host {} to {}.", this.self, contactNode);
    }

    private void uponJoinMessage(JoinMessage msg, Host joiner, short sourceProtoId, int channelId) {
        logger.debug("Host {} contacted me to join the membership. I will try to propose ADD_REPLICA", joiner);
        hostsWaitingToJoin.add(joiner);
        // it's worth considering using double-ended queue and inserting this operation at the front, but it's a detail.
        proposeOrEnqueue(new OperationWrapper(UUID.randomUUID(), OpType.ADD_REPLICA, null, joiner));
    }

    private void uponJoinOKMessage(JoinOKMessage msg, Host from, short sourceProtoId, int channelId){
        logger.info("Join OK received from {}: {}", from, msg);
        state = State.ACTIVE;
        membership = msg.getMembership();
        membership.forEach(this::openConnection);
        highestSequenceNumber = msg.getSequenceNumber();
        currentInstance = msg.getInstance() + 1;
        InstallStateRequest insReq = new InstallStateRequest(msg.getState());
        logger.debug("Sending state to Application");
        sendRequest(insReq, HashApp.PROTO_ID);
        JoinedNotification joinedNotification = new JoinedNotification(msg.getInstance(), highestSequenceNumber);
        logger.debug("Sending joinedNotification");
        triggerNotification(joinedNotification);
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        logger.info("Connection to {} is up", event.getNode());
        initialMembershipConnectionsPending.remove(event.getNode());
    }

    // this will be called when connection goes down. We try again to open connection, if it fails it will
    // be handled by uponOutConnectionFailed
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        logger.error("Connection to {} is down, cause {}", event.getNode(), event.getCause());
        openConnection(event.getNode());
    }

    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed, cause: {}", event.getNode(), event.getCause());

        if (state == State.JOINING) { // well, bad luck, restart the protocol... shouldn't happen tho
            logger.error("Connection to {} failed during JOINING state, cause: {}", event.getNode(), event.getCause());
            return;
        }

        // for first connection to initial membership I want to wait indefinitely - that's to make sure we have time to set up
        // whole network at the beginning
        if (initialMembershipConnectionsPending.contains(event.getNode())) {
            setupTimer(new ConnectionTimer(event.getNode()), waitTimeBetweenReconnects);
            return;
        }

        Integer attempts = connectionsPending.getOrDefault(event.getNode(), 1);
        if (attempts >= maxReconnectAttempts) {
            connectionsPending.remove(event.getNode());
            if (membership.contains(event.getNode())) {
                logger.error("Exceeded max attempts to connect to {}. I will propose to remove this replica.", event.getNode());
                proposeOrEnqueue(new OperationWrapper(UUID.randomUUID(), OpType.REMOVE_REPLICA, null, event.getNode()));
            }
        } else {
            connectionsPending.put(event.getNode(), attempts + 1);
            setupTimer(new ConnectionTimer(event.getNode()), waitTimeBetweenReconnects);
        }
    }

    private void uponConnectionTimer(ConnectionTimer timer, long timerId) {
        logger.debug("Retrying connection to: {}", timer.getHost());
        openConnection(timer.getHost());
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.debug("Connection from {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.debug("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* ---------------- OTHER ---------------------- */

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable, throwable);
    }
}
