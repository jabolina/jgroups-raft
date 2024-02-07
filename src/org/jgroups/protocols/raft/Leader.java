package org.jgroups.protocols.raft;

import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.logging.Log;
import org.jgroups.raft.util.CommitTable;
import org.jgroups.raft.util.RequestTable;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

import java.util.function.Supplier;

/**
 * Implements the behavior of a RAFT leader
 * @author Bela Ban
 * @since  0.1
 */
public class Leader extends RaftImpl {
    protected final Supplier<Integer> majority=() -> raft.majority();

    public Leader(RAFT raft) {
        super(raft);
    }


    public void init() {
        super.init();
        raft.createRequestTable();
        raft.createCommitTable();
    }

    public void destroy() {
        super.destroy();
        RequestTable<String> reqTable = raft.request_table;
        raft.request_table=null;
        raft.commit_table=null;

        if (reqTable != null) reqTable.destroy(raft.notCurrentLeader());
    }


    @Override
    public void handleAppendEntriesResponse(Address sender, long term, AppendResult result) {
        RequestTable<String> reqtab=raft.request_table;
        if(reqtab == null)
            throw new IllegalStateException("request table cannot be null in leader");
        ExtendedUUID uuid=(ExtendedUUID)sender;
        String sender_raft_id=Util.bytesToString(uuid.get(RAFT.raft_id_key));
        Log log=raft.getLog();
        if(log.isTraceEnabled())
            log.trace("%s: received AppendEntries response from %s for term %d: %s", raft.getAddress(), sender, term, result);
        switch(result.result) {
            case OK:
                // Make sure that non-raft members do not count towards majority.
                if (!raft.isRaftMember(sender)) {
                    if (log.isDebugEnabled()) log.debug("%s: dropping vote of non-member %s/%s", raft.getAddress(), sender, raft.members());
                    break;
                }

                raft.commit_table.update(sender, result.index(), result.index() + 1, result.commit_index, false);
                boolean done = reqtab.add(result.index, sender_raft_id, this.majority);
                if(done) {
                    raft.commitLogTo(result.index, true);
                }
                // Send commits immediately.
                // Note that, an entry is committed by a MAJORITY, this means that some of the nodes doesn't know the entry exist yet.
                // This way, send the commit messages any time we handle an append response.
                if(raft.send_commits_immediately) {
                    // Done is only true when reaching a majority threshold, we also need to check is committed to resend
                    // to slower nodes.
                    if (done || reqtab.isCommitted(result.index))
                        sendCommitMessageToFollowers();
                }
                break;
                // todo: change
            case FAIL_ENTRY_NOT_FOUND:
                raft.commit_table.update(sender, result.index(), result.index()+1, result.commit_index, true);
                break;
            case FAIL_CONFLICTING_PREV_TERM:
                raft.commit_table.update(sender, result.index()-1, result.index(), result.commit_index, true, true);
                break;
        }
    }

    private void sendCommitMessageToFollowers() {
        raft.commit_table.forEach(this::sendCommitMessageToFollower);
    }

    private void sendCommitMessageToFollower(Address member, CommitTable.Entry entry) {
        if(raft.commit_index > entry.commitIndex()) {
            long cterm=raft.currentTerm();
            short id=raft.getId();
            Address leader=raft.getAddress();
            Message msg=new ObjectMessage(member, null)
              .putHeader(id, new AppendEntriesRequest(leader, cterm, 0, 0, cterm, raft.commit_index));
            raft.down(msg);
        }
    }
}
