package protocols.agreement.model;

import protocols.agreement.messages.AcceptOkMessage;
import protocols.agreement.messages.PrepareOKMessage;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class InstanceState {
    private OperationWrapper proposerValue = OperationWrapper.NULL;
    private long proposerSeq;
    private long highestPrepare = -1;
    private long highestAccept = -1;
    private OperationWrapper highestValue = OperationWrapper.NULL;
    private List<PrepareOKMessage> prepareOkSet = new LinkedList<>(); //set of PrepareOKMessages (with current sequence number)
    private List<AcceptOkMessage> acceptOkSet = new LinkedList<>(); //set of AcceptOKMessages (with the highest sequence number)
    private OperationWrapper decided; //locally decided op
    private Set<Host> allProcesses = new HashSet<>(); //contains all processes

    public InstanceState(long proposerSeq) {
        //this.proposerSeq = processId; //todo: maybe not needed, sendMessage automatically sends processId
        this.proposerSeq = proposerSeq;
    }

    public OperationWrapper getProposerValue() {
        return proposerValue;
    }

    public void setProposerValue(OperationWrapper proposerValue) {
        this.proposerValue = proposerValue;
    }

    public long getProposerSeq() {
        return proposerSeq;
    }

    public void setProposerSeq(long proposerSeq) {
        this.proposerSeq = proposerSeq;
    }

    public long getHighestPrepare() {
        return highestPrepare;
    }

    public void setHighestPrepare(long highestPrepare) {
        this.highestPrepare = highestPrepare;
    }

    public long getHighestAccept() {
        return highestAccept;
    }

    public void setHighestAccept(long highestAccept) {
        this.highestAccept = highestAccept;
    }

    public List<PrepareOKMessage> getPrepareOkSet() {
        return prepareOkSet;
    }

    public void setPrepareOkSet(List<PrepareOKMessage> prepareOkSet) {
        this.prepareOkSet = prepareOkSet;
    }

    public List<AcceptOkMessage> getAcceptOkSet() {
        return acceptOkSet;
    }

    public void setAcceptOkSet(List<AcceptOkMessage> acceptOkSet) {
        this.acceptOkSet = acceptOkSet;
    }

    public OperationWrapper getDecided() {
        return decided;
    }

    public void setDecided(OperationWrapper decided) {
        this.decided = decided;
    }

    public Set<Host> getAllProcesses() {
        return allProcesses;
    }

    public void setAllProcesses(Set<Host> allProcesses) {
        this.allProcesses = allProcesses;
    }

    public OperationWrapper getHighestValue() {
        return highestValue;
    }

    public void setHighestValue(OperationWrapper highestValue) {
        this.highestValue = highestValue;
    }
}
