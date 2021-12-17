package protocols.agreement;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.agreement.messages.AcceptMessage;
import protocols.agreement.messages.AcceptOKMessage;
import protocols.agreement.messages.BroadcastMessage;
import protocols.agreement.messages.PrepareOKMessage;
import protocols.agreement.messages.PrepareMessage;
import protocols.agreement.notifications.DecidedNotification;
import protocols.agreement.notifications.JoinedNotification;
import protocols.agreement.requests.AddReplicaRequest;
import protocols.agreement.requests.ProposeRequest;
import protocols.agreement.requests.RemoveReplicaRequest;
import protocols.app.utils.Operation;
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
    public final static String PROTOCOL_NAME = "EmptyAgreement";

    private Host myself;
    private int joinedInstance;
    private Operation proposerOp; //value to be proposed
    private long proposerSeq; //current sequence number of proposer
    private long highestPrepSeq; //highest prepare sequence number
    private long highestAcceptSeq; //highest accept sequence number
    private Operation highestOp; //op accepted with the highest sequence number
    private Collection<PrepareOKMessage> prepareOKSet; //set of PrepareOKMessages (with current sequence number)
    private Collection<AcceptOKMessage> acceptOKSet; //set of AcceptOKMessages (with the highest sequence number)
    private Operation decided; //locally decided op
    private List<Host> membership; //contains all processes


    public Paxos(Properties props) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        joinedInstance = -1; //-1 means we have not yet joined the system
        membership = null;

        /*--------------------- Register Timer Handlers ----------------------------- */

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
        //proposerSeq <- localProcessID
        //allProcesses <- membership?
        proposerOp = null;
        highestPrepSeq = 0;
        highestAcceptSeq = 0;
        highestOp = null;
        decided = null;
    }

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
        try {
              registerMessageHandler(cId, BroadcastMessage.MSG_ID, this::uponBroadcastMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            throw new AssertionError("Error registering message handler.", e);
        }

    }

    private void uponBroadcastMessage(BroadcastMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            //Obviously your agreement protocols will not decide things as soon as you receive the first message
            triggerNotification(new DecidedNotification(msg.getInstance(), msg.getOpId(), msg.getOp()));
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponPrepareMessage(PrepareMessage msg, Host sender, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            if(msg.getSequenceNumber() > highestPrepSeq){
                highestPrepSeq = msg.getSequenceNumber();
                PrepareOKMessage prepOKmsg = new PrepareOKMessage(msg.getInstance(), msg.getOpId(),  msg.getSequenceNumber(), highestAcceptSeq, highestOp);
                sendMessage(prepOKmsg, sender);
            }
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponPrepareOKMessage(PrepareOKMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            if(proposerSeq > msg.getSequenceNumber()){
                prepareOKSet.add(msg);
                if(prepareOKSet.size() > ((membership.size() / 2) + 1)){
                    //{hna, hva} <- Highest(prepareOKSet)
                    if(highestOp != null)
                        proposerOp = highestOp;
                    AcceptMessage acceptMsg = new AcceptMessage(proposerSeq, proposerOp);
                    for (Host h : membership) {
                        sendMessage(acceptMsg, h);
                    }
                    prepareOKSet.clear();
                }
                //TODO: Cancel Timer Timeout
                //TODO: Setup Timer Timeout (T)
            }
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponAcceptMessage(AcceptMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            if(msg.getSequenceNumber() > highestPrepSeq){
                highestPrepSeq = msg.getSequenceNumber();
                highestAcceptSeq = msg.getSequenceNumber();
                highestOp = msg.getOp();
                AcceptOKMessage acceptOKMsg = new AcceptOKMessage(msg.getSequenceNumber(), msg.getOp());
                for (Host h : membership) {
                    sendMessage(acceptOKMsg, h);
                }
            }
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    private void uponAcceptOKMessage(AcceptOKMessage msg, Host host, short sourceProto, int channelId) {
        if(joinedInstance >= 0 ){
            //if FORALL(hsn, hsv) in AcceptOKSet: hsn = sn AND hv = v THEN
                acceptOKSet.add(msg); //msg -> {sn, v}
            //else if EXISTS(hsn,hv) in AcceptOKSet: hsn < sn THEN
                acceptOKSet.clear();
                acceptOKSet.add(msg); //msg -> {sn, v}
            if (decided == null && (acceptOKSet.size() >= ((membership.size() / 2) + 1))){
                decided = msg.getOp();
                //triggerNotification(DecidedNotification);
                if(msg.getSequenceNumber() == proposerSeq)
                    //Cancel Timer Timeout
            }
        } else {
            //We have not yet received a JoinedNotification, but we are already receiving messages from the other
            //agreement instances, maybe we should do something with them...?
        }
    }

    /** upon Timer Timeout do
     * {
     *  if decided == null then
     *      proposer_seq <- proposer_seq + #allProcesses
     *      Foreach p in allProcesses do:
     *          Send PREPARE (p, proposer_seq)
     *      prepareOKSet.clear()
     *      setup timer TImeout (T)
     * }
     */

    private void uponJoinedNotification(JoinedNotification notification, short sourceProto) {
        //We joined the system and can now start doing things
        joinedInstance = notification.getJoinInstance();
        membership = new LinkedList<>(notification.getMembership());
        logger.info("Agreement starting at instance {},  membership: {}", joinedInstance, membership);
    }

    private void uponProposeRequest(ProposeRequest request, short sourceProto) {
        logger.debug("Received " + request);
        PrepareMessage msg = new PrepareMessage(request.getInstance(), request.getOpId(), request.getOperation());
        logger.debug("Sending to: " + membership);
        membership.forEach(h -> sendMessage(msg, h));
        prepareOKSet = null;
        //TODO: Setup Timer Timeout(T)
    }
    private void uponAddReplica(AddReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The AddReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.add(request.getReplica());
    }
    private void uponRemoveReplica(RemoveReplicaRequest request, short sourceProto) {
        logger.debug("Received " + request);
        //The RemoveReplicaRequest contains an "instance" field, which we ignore in this incorrect protocol.
        //You should probably take it into account while doing whatever you do here.
        membership.remove(request.getReplica());
    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

}
