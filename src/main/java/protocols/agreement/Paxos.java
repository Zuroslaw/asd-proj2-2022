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
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class Paxos extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(Paxos.class);

    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "Paxos";

    private int joinedInstance;

    private final long TIMEOUT;
    private final Map<Integer, InstanceState> instances = new HashMap<>();
    private final Map<Integer, Long> instanceToTimerId = new HashMap<>();

    private long initialSequenceNumber;

    public Paxos(Properties props) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        TIMEOUT = Long.parseLong(props.getProperty("paxos_timeout_ms", "2000"));
        joinedInstance = -1; //-1 means we have not yet joined the system
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(TimeoutTimer.TIMER_ID, this::uponTimerTimout);

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(ProposeRequest.REQUEST_ID, this::uponProposeRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(ChannelReadyNotification.NOTIFICATION_ID, this::uponChannelCreated);
        subscribeNotification(JoinedNotification.NOTIFICATION_ID, this::uponJoinedNotification);
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelReadyNotification notification, short sourceProto) {
        int cId = notification.getChannelId();
        logger.info("Channel {} created, I am {}", cId, notification.getMyself());
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(cId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(cId, PrepareMessage.MSG_ID, PrepareMessage.serializer);
        registerMessageSerializer(cId, PrepareOKMessage.MSG_ID, PrepareOKMessage.serializer);
        registerMessageSerializer(cId, AcceptMessage.MSG_ID, AcceptMessage.serializer);
        registerMessageSerializer(cId, AcceptOkMessage.MSG_ID, AcceptOkMessage.serializer);
        registerMessageSerializer(cId, AbandonProposalMessage.MSG_ID, AbandonProposalMessage.serializer);
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(cId, PrepareMessage.MSG_ID, this::uponPrepareMessage, this::uponMsgFail);
            registerMessageHandler(cId, PrepareOKMessage.MSG_ID, this::uponPrepareOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptMessage.MSG_ID, this::uponAcceptMessage, this::uponMsgFail);
            registerMessageHandler(cId, AcceptOkMessage.MSG_ID, this::uponAcceptOkMessage, this::uponMsgFail);
            registerMessageHandler(cId, AbandonProposalMessage.MSG_ID, this::uponAbandonProposalMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error(e);
        }
    }

    @Override
    public void init(Properties props) {
        logger.debug("Paxos initialized");
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        int instance = request.getInstance();
        if (instances.containsKey(instance)) {
            logger.debug("Received propose request, but I already participate in this instance. Ignoring.");
            // It happens when someone already proposed, my proposer value is null op. Well maybe I could still propose this?
            return;
        }
        logger.debug("Handling propose request: {}", request);

        // setup instance state
        InstanceState instanceState = new InstanceState(initialSequenceNumber);
        instanceState.setProposerValue(request.getOperation());
        instanceState.setAllProcesses(request.getMembership());
        instances.put(instance, instanceState);

        // send prepare to all processes
        PrepareMessage msg = new PrepareMessage(instance, instanceState.getProposerSeq());
        logger.debug("Sending prepare message to all processes [{}]: {}", instanceState.getAllProcesses(), msg);
        instanceState.getAllProcesses()
                .forEach(host -> sendMessage(msg, host));

        // setup timer
        long timerId = setupTimer(new TimeoutTimer(instance), TIMEOUT);
        instanceToTimerId.put(instance, timerId);
    }

    private boolean shouldNotParticipate(int instanceNumber) {
        // As described in long comment in StateMachine class, maybe I could do something like caching
        // messages when joinedInstance == -1. But the solution in StateMachine works, and I think it is also safer
        return joinedInstance < 0 || joinedInstance >= instanceNumber;
    }

    private void uponPrepareMessage(PrepareMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            logger.debug("I should not participate in this instance, ignoring msg from [{}]: {}", host, msg);
            return;
        }
        logger.debug("Handling prepare message from [{}]: {}", host, msg);

        InstanceState instance = instances.get(msg.getInstance());
        if (instance == null) {
            logger.debug("Creating new instance number {}, as I did not receive client order yet", msg.getInstance());
            instance = new InstanceState(initialSequenceNumber); // I don't have membership yet, but it's ok
            instances.put(msg.getInstance(), instance);
        }

        if (msg.getSequenceNumber() > instance.getHighestPrepare()) {
            logger.debug("Updating highest prepare to: {}", msg.getSequenceNumber());
            instance.setHighestPrepare(msg.getSequenceNumber());
            PrepareOKMessage prepareOk = new PrepareOKMessage(msg.getInstance(), msg.getSequenceNumber(), instance.getHighestAccept(), instance.getHighestValue());
            logger.debug("Sending prepareOk message back: {}", prepareOk);
            sendMessage(prepareOk, host);
        } else {
            logger.debug("I already promised higher number, I will tell the proposer to abandon its proposal");
            // this is an optimization described in "Paxos Made Simple" page 6
            AbandonProposalMessage abandonMessage = new AbandonProposalMessage(msg.getInstance(), msg.getSequenceNumber());
            sendMessage(abandonMessage, host);
        }
    }

    private void uponPrepareOkMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            logger.debug("I should not participate in this instance, ignoring msg from [{}]: {}", host, msg);
            return;
        }
        logger.debug("Handling prepareOk message from [{}]: {}", host, msg);
        // At this point, I have membership because PrepareOK message means we needed to send Prepare first
        // and that means we got membership from State Machine
        InstanceState instance = instances.get(msg.getInstance());
        // I think checking if sender is in membership is not necessary. After all, we can get PrepareOk only if we sent Prepare,
        // and we sent Prepare only to valid members in that instance. But I put it here for some reason, and now I'm scared to remove it.
        if (instance.getAllProcesses().contains(host) && instance.getProposerSeq() == msg.getSequenceNumber()) {
            logger.debug("Adding message to PrepareOk set");
            instance.getPrepareOkSet().add(msg);
            if (instance.getPrepareOkSet().size() >= majority(instance)) {
                logger.debug("Got majority of prepare OK messages: {}", instance.getPrepareOkSet().size());

                // choose the highest accepted value
                PrepareOKMessage highest = highest(instance.getPrepareOkSet());
                if (!OperationWrapper.NULL.equals(highest.getHighestAcceptedValue())) {
                    logger.debug("There is already accepted value, setting my proposition to it: {}", highest.getHighestAcceptedValue());
                    instance.setProposerValue(highest.getHighestAcceptedValue());
                }

                // send accept messages to all members
                AcceptMessage accept = new AcceptMessage(msg.getInstance(), instance.getProposerSeq(), instance.getProposerValue(), instance.getAllProcesses());
                logger.debug("Sending accept to all processes [{}]: {}", instance.getAllProcesses(), accept);
                instance.getAllProcesses()
                        .forEach(p -> sendMessage(accept, p));
                instance.setPrepareOkSet(new LinkedList<>());

                // reset timer
                long timerId = instanceToTimerId.get(msg.getInstance());
                cancelTimer(timerId);
                setupTimer(new TimeoutTimer(msg.getInstance()), TIMEOUT);
            }
        }
    }

    private void uponAbandonProposalMessage(AbandonProposalMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            logger.debug("I should not participate in this instance, ignoring msg from [{}]: {}", host, msg);
            return;
        }
        InstanceState instance = instances.get(msg.getInstance());
        if (msg.getSequenceNumber() == instance.getProposerSeq()) {
            if (instance.getDecided() != null) {
                // increment seq number, resend proposal and reset timer
                ProtoTimer timer = cancelTimer(instanceToTimerId.get(msg.getInstance()));
                resendProposal(msg.getInstance());
                long newTimerId = setupTimer(timer, TIMEOUT);
                instanceToTimerId.put(msg.getInstance(), newTimerId);
            }
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            logger.debug("I should not participate in this instance, ignoring msg from [{}]: {}", host, msg);
            return;
        }
        logger.debug("Handling accept message from [{}]: {}", host, msg);

        InstanceState instance = instances.get(msg.getInstance());
        if (instance == null) {
            logger.debug("Creating new instance number {}, as I did not receive client order yet", msg.getInstance());
            instance = new InstanceState(initialSequenceNumber);
            instances.put(msg.getInstance(), instance);
        }
        instance.setAllProcesses(msg.getMembership());

        if (msg.getSequenceNumber() >= instance.getHighestPrepare()) { // I promised to accept only above highestPrepare
            logger.debug("Sequence number is higher or equal to the one I promised, I can accept this proposal.");
            logger.debug("Setting highest prepare, highest accept and highest value");
            instance.setHighestPrepare(msg.getSequenceNumber());
            instance.setHighestAccept(msg.getSequenceNumber());
            instance.setHighestValue(msg.getValue());
            AcceptOkMessage acceptOk = new AcceptOkMessage(msg.getInstance(), msg.getSequenceNumber(), msg.getValue(), instance.getAllProcesses());
            logger.debug("Sending AcceptOk to all processes [{}]: {}", instance.getAllProcesses(), acceptOk);
            instance.getAllProcesses()
                    .forEach(p -> sendMessage(acceptOk, p));
        } else {
            logger.debug("I already promised higher number, I will tell the proposer to abandon its proposal");
            AbandonProposalMessage abandonMessage = new AbandonProposalMessage(msg.getInstance(), msg.getSequenceNumber());
            sendMessage(abandonMessage, host);
        }
    }

    private void uponAcceptOkMessage(AcceptOkMessage msg, Host host, short sourceProto, int channelId) {
        if (shouldNotParticipate(msg.getInstance())) {
            logger.debug("I should not participate in this instance, ignoring msg from [{}]: {}", host, msg);
            return;
        }
        logger.debug("Handling acceptOk message from [{}]: {}", host, msg);

        InstanceState instance = instances.get(msg.getInstance());
        if (instance == null) {
            logger.debug("Creating new instance number {}, as I did not receive client order yet", msg.getInstance());
            instance = new InstanceState(initialSequenceNumber);
            instances.put(msg.getInstance(), instance);
        }
        // I think we need to pass membership in Accept Ok too (just as in Accept). It might be possible that we miss
        // all Accept messages, and yet get Accept Oks, in which case we won't have membership.
        instance.setAllProcesses(msg.getMembership());

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
            logger.debug("Got majority of accept oks. Deciding value and sending decided notification: {}", notification);
            triggerNotification(notification);
            if (msg.getSequenceNumber() == instance.getProposerSeq()) {
                cancelTimer(instanceToTimerId.get(msg.getInstance()));
            }
        }
    }

    private void uponTimerTimout(TimeoutTimer timer, long timerId) {
        InstanceState instance = instances.get(timer.getInstance());
        if (instance.getDecided() == null) {
            logger.debug("Timer timed out.");
            resendProposal(timer.getInstance());
            long newTimerId = setupTimer(timer, TIMEOUT);
            instanceToTimerId.put(timer.getInstance(), newTimerId);
        }
    }

    private void resendProposal(int instanceNumber) {
        InstanceState instance = instances.get(instanceNumber);
        instance.setProposerSeq(instance.getProposerSeq() + instance.getAllProcesses().size());
        PrepareMessage msg = new PrepareMessage(instanceNumber, instance.getProposerSeq());
        logger.debug("Incrementing sequence number to: {} and sending new prepare message: {}", instance.getProposerSeq(), msg);
        instance.getAllProcesses()
                .forEach(host -> sendMessage(msg, host));
        instance.setPrepareOkSet(new LinkedList<>());
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

    /* --------------------------- */

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
