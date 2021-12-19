package protocols.agreement;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.*;
import protocols.agreement.model.InstanceState;
import protocols.agreement.model.OpType;
import protocols.agreement.model.OperationWrapper;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.agreement.timers.TimeoutTimer;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * This is NOT a correct agreement protocol (it is actually a VERY wrong one)
 * This is simply an example of things you can do, and can be used as a starting point.
 *
 * You are free to change/delete ANYTHING in this class, including its fields.
 * Do not assume that any logic implemented here is correct, think for yourself!
 */
public class Paxos extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Paxos.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Paxos";

    private Host myself;
    private int joinedInstance;

    private final long TIMEOUT = 2000; //todo, maybe parameter
    private Map<Integer, InstanceState> instances;
    private Map<Integer, Long> instanceToTimerId;
    private Set<Host> newestMembership;
    private long initialSequenceNumber; // todo it should be 0,1,2,3 ... ? passed by state machine?
    private Map<Integer, Queue<Runnable>> messageQueues;


    public Paxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        this.initialSequenceNumber = myself.hashCode(); // todo ask if it should be passed? otherwise increasing seq in timer doesnt make sense for big numbers?
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(TimeoutTimer.TIMER_ID, this::uponTimerTimout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);
        registerRequestHandler(AddReplicaRequest.REQUEST_ID, this::uponAddReplica);
        registerRequestHandler(RemoveReplicaRequest.REQUEST_ID, this::uponRemoveReplica);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        this.instances = new HashMap<>();
        this.messageQueues = new HashMap<>();
    }

    private void enqueueMessageHandling(Runnable task, int instance) {
        if (messageQueues.containsKey(instance)) {
            messageQueues.get(instance).offer(task);
        } else {
            Queue<Runnable> newMessageQueue = new LinkedList<>();
            newMessageQueue.offer(task);
            messageQueues.put(instance, newMessageQueue);
        }
    }

    private void invokeMessageQueue(int instance) {
        Queue<Runnable> queue = messageQueues.get(instance);
        Runnable task;
        while ((task = queue.poll()) != null) {
            task.run();
        }
    }

    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        OperationWrapper op = new OperationWrapper(request.getOpId(), OpType.ADD_REPLICA, null, request.getReplica());
        handleNewProposal(request.getInstance(), op);
    }

    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        OperationWrapper op = new OperationWrapper(request.getOpId(), OpType.REMOVE_REPLICA, null, request.getReplica());
        handleNewProposal(request.getInstance(), op);
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        OperationWrapper op = new OperationWrapper(request.getOpId(), OpType.CLIENT_REQUEST, request.getOperation(), null);
        handleNewProposal(request.getInstance(), op);
    }

    private void handleNewProposal(int instance, OperationWrapper operationWrapper) {
        InstanceState instanceState = new InstanceState(initialSequenceNumber, newestMembership);
        instanceState.setProposerValue(operationWrapper);
        instances.put(instance, instanceState);

        if (!messageQueues.containsKey(instance)) { // check if we received any messages for this "new" instance so far
            invokeMessageQueue(instance);
            if (instanceState.getDecided() != null) {
                // it may happen that inside message queue there were acceptOk messages from the majority of processes (value decided)
                // in which case it doesn't make sense to proceed with proposal, and what is more important we shouldn't set up timer
                // (because that means timer for this instance will go forever and will send requests forever, which doesn't break correctness,
                // but is unnecessary and can increase network traffic)
                return;
            }
        }

        PrepareMessage msg = new PrepareMessage(instance, instanceState.getProposerSeq());
        logger.debug("Sending to: {}", newestMembership);
        instanceState.getAllProcesses()
                .forEach(host -> sendMessage(msg, host));
        long timerId = setupTimer(new TimeoutTimer(instance), TIMEOUT);
        instanceToTimerId.put(instance, timerId);
    }

    /*
        TODO: how to manage joining instances? Let's say in instance 1 we have 3 nodes. My node gets partitioned, other
        two will decide to join 4th instance.
        // todo: up relevant only if we don't enqueue messages
     */
    //TODO: what if node does not receive acceptOks in given time? It will not send decide back up and it will be stuck?
    // but then connection is still up, so nobody knows it is stuck? maybe we can respond with decided value on prepare msg?

    private boolean shouldNotParticipate(int instanceNumber) {
        return joinedInstance < 0 || instanceNumber < joinedInstance;
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        InstanceState instance = instances.get(msg.getInstance());
        if (msg.getInstance() < joinedInstance) {
            return;
        }
        if (instance == null) { // i don't have to check if joinedInstance < 0, because in that case instance is always null
            //todo: if i didnt join yet, i will enqueue this msg. But then if this msg instance is lower than my
            // joinedInstance, i will never run this task, so i have garbage in my messageQueues. Maybe we can clean
            // it up in uponJoinNotification?
            enqueueMessageHandling(() -> uponPrepareMessage(msg, host, sourceProto, channelId), msg.getInstance());
            return;
        }
        if (!instance.getAllProcesses().contains(host)) {
            return;
        }
        if (msg.getSequenceNumber() > instance.getHighestPrepare()) { // I already promised higher number, don't respond to that
            instance.setHighestPrepare(msg.getSequenceNumber());
            PrepareOKMessage prepareOk = new PrepareOKMessage(msg.getInstance(), msg.getSequenceNumber(), instance.getHighestAccept(), instance.getHighestValue());
            sendMessage(prepareOk, host);
        }
        /*
            Possible optimization here - add else clause and send back "abandon" message to proposer, who will abandon its proposed value
            this is described in "Paxos made simple" page 6
         */
    }

    private void uponPrepareOkMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        InstanceState instance = instances.get(msg.getInstance());
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        if (instance == null) {
            enqueueMessageHandling(() -> uponPrepareOkMessage(msg, host, sourceProto, channelId), msg.getInstance());
            return;
        }
        if (!instance.getAllProcesses().contains(host)) {
            return;
        }
        if (instance.getProposerSeq() == msg.getSequenceNumber()) { // I only care about my current proposal
            instance.getPrepareOkSet().add(msg);
            if (instance.getPrepareOkSet().size() > majority(instance)) {
                PrepareOKMessage highest = highest(instance.getPrepareOkSet());
                if (highest != null) {
                    instance.setProposerValue(highest.getHighestAcceptedValue());
                }
                AcceptMessage accept = new AcceptMessage(msg.getInstance(), instance.getProposerSeq(), instance.getProposerValue());
                instance.getAllProcesses()
                        .forEach(p -> sendMessage(accept, p));
                instance.setPrepareOkSet(new LinkedList<>());
                long timerId = instanceToTimerId.get(msg.getInstance());
                cancelTimer(timerId);
                setupTimer(new TimeoutTimer(msg.getInstance()), TIMEOUT);
            }
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        InstanceState instance = instances.get(msg.getInstance());
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        if (instance == null) {
            enqueueMessageHandling(() -> uponAcceptMessage(msg, host, sourceProto, channelId), msg.getInstance());
            return;
        }
        if (!instance.getAllProcesses().contains(host)) {
            return;
        }
        if (msg.getSequenceNumber() >= instance.getHighestPrepare()) { // I promised to accept only above highestPrepare
            instance.setHighestPrepare(msg.getSequenceNumber());
            instance.setHighestAccept(msg.getSequenceNumber());
            instance.setHighestValue(msg.getValue());
            AcceptOkMessage acceptOk = new AcceptOkMessage(msg.getInstance(), msg.getSequenceNumber(), msg.getValue());
            instance.getAllProcesses()
                    .forEach(p -> sendMessage(acceptOk, p));
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId) {
        InstanceState instance = instances.get(msg.getInstance());
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        if (instance == null) {
            enqueueMessageHandling(() -> uponAcceptOkMessage(msg, host, sourceProto, channelId), msg.getInstance());
            return;
        }
        if (!instance.getAllProcesses().contains(host)) {
            return;
        }
        if (allAcceptOksEqual(instance, msg)) {
            instance.getAcceptOkSet().add(msg);
        } else if (anyAcceptOkIsSmaller(instance, msg)) {
            instance.getAcceptOkSet().clear();
            instance.getAcceptOkSet().add(msg);
        }
        if (instance.getDecided() == null && instance.getAcceptOkSet().size() >= majority(instance)) {
            instance.setDecided(msg.getValue());
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getValue().getOpId(), msg.getValue().getOpType(), msg.getValue().getOperation(), host));
            if (msg.getSequenceNumber() == instance.getProposerSeq()) {
                cancelTimer(instanceToTimerId.get(msg.getInstance()));
            }
        }
    }

    private void uponTimerTimout(TimeoutTimer timer, long timerId) {
        InstanceState instance = instances.get(timer.getInstance());
        if (instance.getDecided() == null) {
            instance.setProposerSeq(instance.getProposerSeq() + instance.getAllProcesses().size());
            PrepareMessage msg = new PrepareMessage(timer.getInstance(), instance.getProposerSeq());
            instance.getAllProcesses()
                    .forEach(host -> sendMessage(msg, host));
            instance.setPrepareOkSet(new LinkedList<>());
            long newTimerId = setupTimer(timer, TIMEOUT);
            instanceToTimerId.put(timer.getInstance(), newTimerId);
        }
    }

    private boolean allAcceptOksEqual(InstanceState instance, AcceptOkMessage newMsg) {
        return instance.getAcceptOkSet().stream()
                .allMatch(msg -> msg.getSequenceNumber() == newMsg.getSequenceNumber() && msg.getValue().equals(newMsg.getValue()));
    }

    private boolean anyAcceptOkIsSmaller(InstanceState instance, AcceptOkMessage newMsg) {
        return instance.getAcceptOkSet().stream()
                .anyMatch(msg -> msg.getSequenceNumber() < newMsg.getSequenceNumber());
    }

    private int majority(InstanceState instance) {
        return (instance.getAllProcesses().size() / 2) + 1;
    }

    private PrepareOKMessage highest(Collection<PrepareOKMessage> messages) {
        return messages.stream()
                .max(Comparator.comparing(PrepareOKMessage::getHighestAcceptedSeq))
                .orElse(null);
    }

    /** --------------------------- */

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        myself = notification.getMyself();
        logger.info("Channel {} created, I am {}", cId, myself);
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, BroadcastMessage.MSG_ID, BroadcastMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */

    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        newestMembership = new HashSet<>(notification.getMembership());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
