package protocols.agreement;

import java.io.IOException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.*;
import protocols.agreement.model.InstanceState;
import protocols.agreement.model.OperationWrapper;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.timers.TimeoutTimer;
import protocols.statemachine.notifications.ChannelReadyNotification;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class Paxos extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Paxos.class);

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Paxos";

    private Host myself;
    private int joinedInstance;

    private final long TIMEOUT;
    private final Map<Integer, InstanceState> instances = new HashMap<>();
    private final Map<Integer, Long> instanceToTimerId = new HashMap<>();
    private Set<Host> newestMembership;

    /*
        todo:
        - i need to have membership in acceptOk msg too, right? I can miss every message up to the point of acceptOk, and then when it comes I need to calculate majority
        - i need to check if an instance is in membership only in uponPrepareOkMessage? other places are secured
        - i need to handle somehow situation when i missed all of the messages of given instance and i don't have any client request - in that case i will be stuck
     */
    private long initialSequenceNumber;

    public Paxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        String timeout = props.getProperty("paxos_timeout_ms", "2000");
        TIMEOUT = Long.parseLong(timeout);
        joinedInstance = -1; //-1 means we have not yet joined the system
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(TimeoutTimer.TIMER_ID, this::uponTimerTimout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    @Override
    public void init(Properties props) {
        logger.debug("Paxos initialized");
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        int instance = request.getInstance();
        if (instances.containsKey(instance)) {
            logger.debug("Received propose request, but I already participate in this instance. Ignoring.");
            return; // i'm already participating in this instance, ignore
        }
        logger.debug("Handling propose request: {}", request);
        InstanceState instanceState = new InstanceState(initialSequenceNumber, request.getMembership());
        instanceState.setProposerValue(request.getOperation());
        instances.put(instance, instanceState);

        PrepareMessage msg = new PrepareMessage(instance, instanceState.getProposerSeq());
        logger.debug("Sending prepare message to all processes: {}", msg);
        instanceState.getAllProcesses()
                .forEach(host -> sendMessage(msg, host));
        long timerId = setupTimer(new TimeoutTimer(instance), TIMEOUT);
        instanceToTimerId.put(instance, timerId);
    }

    private boolean shouldNotParticipate(int instanceNumber) {
        return joinedInstance < 0 || joinedInstance >= instanceNumber;
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        logger.debug("Handling prepare message from [{}]: {}", host, msg);
        InstanceState instance = instances.get(msg.getInstance());
        if (instance == null) {
            logger.debug("Creating new instance number {}, as I did not receive client order yet", msg.getInstance());
            instance = new InstanceState(initialSequenceNumber, new HashSet<>());
            instances.put(msg.getInstance(), instance);
        }
        if (msg.getSequenceNumber() > instance.getHighestPrepare()) { // I already promised higher number, don't respond if its smaller
            logger.debug("Updating highest prepare to: {}", msg.getSequenceNumber());
            instance.setHighestPrepare(msg.getSequenceNumber());
            PrepareOKMessage prepareOk = new PrepareOKMessage(msg.getInstance(), msg.getSequenceNumber(), instance.getHighestAccept(), instance.getHighestValue());
            logger.debug("Sending prepareOk message back: {}", prepareOk);
            sendMessage(prepareOk, host);
        } else {
            logger.debug("I already promised higher number, I will not respond to this prepare message");
        }
    }

    private void uponPrepareOkMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        logger.debug("Handling prepareOk message from [{}]: {}", host, msg);
        InstanceState instance = instances.get(msg.getInstance());
        if (instance != null && instance.getAllProcesses().contains(host) && instance.getProposerSeq() == msg.getSequenceNumber()) { // I only care about my current proposal
            logger.debug("Adding message to PrepareOk set");
            instance.getPrepareOkSet().add(msg);
            if (instance.getPrepareOkSet().size() >= majority(instance)) {
                logger.debug("Got majority of prepare OK messages: {}", instance.getPrepareOkSet().size());
                PrepareOKMessage highest = highest(instance.getPrepareOkSet());
                if (!Objects.equals(highest.getHighestAcceptedValue(), OperationWrapper.nullOperation())) {
                    logger.debug("There is already accepted value, setting my proposition to it: {}", highest.getHighestAcceptedValue());
                    instance.setProposerValue(highest.getHighestAcceptedValue());
                }
                AcceptMessage accept = new AcceptMessage(msg.getInstance(), instance.getProposerSeq(), instance.getProposerValue(), instance.getAllProcesses());
                logger.debug("Sending accept to all processes: {}", accept);
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
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        logger.debug("Handling accept message from [{}]: {}", host, msg);
        InstanceState instance = instances.get(msg.getInstance());
        if (instance == null) {
            logger.debug("Creating new instance number {}, as I did not receive client order yet", msg.getInstance());
            instance = new InstanceState(initialSequenceNumber, msg.getMembership());
            instances.put(msg.getInstance(), instance);
        }
        instance.setAllProcesses(msg.getMembership()); // todo refactor
        if (msg.getSequenceNumber() >= instance.getHighestPrepare()) { // I promised to accept only above highestPrepare
            logger.debug("Sequence number is higher or equal to the one I promised, I can accept this proposal.");
            logger.debug("Setting highest prepare, highest accept and highest value");
            instance.setHighestPrepare(msg.getSequenceNumber());
            instance.setHighestAccept(msg.getSequenceNumber());
            instance.setHighestValue(msg.getValue());
            AcceptOkMessage acceptOk = new AcceptOkMessage(msg.getInstance(), msg.getSequenceNumber(), msg.getValue(), instance.getAllProcesses());
            logger.debug("Sending AcceptOk to all processes: {}", acceptOk);
            instance.getAllProcesses()
                    .forEach(p -> sendMessage(acceptOk, p));
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            return;
        }
        logger.debug("Handling acceptOk message from [{}]: {}", host, msg);
        InstanceState instance = instances.get(msg.getInstance());
        if (instance == null) {
            logger.debug("Creating new instance number {}, as I did not receive client order yet", msg.getInstance());
            instance = new InstanceState(initialSequenceNumber, msg.getMembership());
            instances.put(msg.getInstance(), instance);
        }
        instance.setAllProcesses(msg.getMembership()); // todo refactor
        if (allAcceptOksEqual(instance, msg)) {
            logger.debug("It's another accept OK, same as all before (or there were no accept OK's yet)");
            instance.getAcceptOkSet().add(msg);
        } else if (anyAcceptOkIsSmaller(instance, msg)) {
            logger.debug("It's different AcceptOk than others, resetting accept ok set");
            instance.getAcceptOkSet().clear();
            instance.getAcceptOkSet().add(msg);
        }
        if (instance.getDecided() == null && instance.getAcceptOkSet().size() >= majority(instance)) {
            instance.setDecided(msg.getValue());
            DecidedNotification notification = new DecidedNotification(msg.getInstance(), msg.getValue());
            logger.debug("Got majority of accept oks. Deciding value and sending decided notfication: {}", notification);
            triggerNotification(notification);
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
            logger.debug("Timer timed out. Incrementing sequence number to: {} and sending new prepare message: {}", instance.getProposerSeq(), msg);
            instance.getAllProcesses()
                    .forEach(host -> sendMessage(msg, host));
            instance.setPrepareOkSet(new LinkedList<>());
            long newTimerId = setupTimer(timer, TIMEOUT);
            instanceToTimerId.put(timer.getInstance(), newTimerId);
        }
    }

    private boolean allAcceptOksEqual(InstanceState instance, AcceptOkMessage newMsg) {
        return instance.getAcceptOkSet().stream()
                .allMatch(msg -> Objects.equals(msg.getSequenceNumber(), newMsg.getSequenceNumber()) && Objects.equals(msg.getValue(), newMsg.getValue()));
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
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error(e);
        }

    }

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        joinedInstance = notification.getJoinInstance();
        initialSequenceNumber = notification.getSequenceNumber();
        logger.info("Joined in instance: {} with sequence number: {}", joinedInstance, initialSequenceNumber);
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
