package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.View;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.raft.testfwk.RaftTestUtils;
import org.jgroups.util.ExtendedUUID;
import org.jgroups.util.Util;

/**
 * @author Bela Ban
 * @since  1.0.6
 */
public class Utils {
    public enum Majority {
        reached, lost, leader_lost, no_change
    }

    public static boolean majorityReached(View old, View new_view, int majority) {
        return (old == null || old.size() < majority) && new_view.size() >= majority;
    }

    public static boolean majorityLost(View old, View new_view, int majority) {
        return (old != null && old.size() >= majority) && new_view.size() < majority;
    }

    public static Majority computeMajority(View old, View new_view, int majority, Address leader) {
        if(majorityReached(old, new_view, majority))
            return Majority.reached;
        if(majorityLost(old, new_view, majority))
            return Majority.lost;
        if(leader != null && !new_view.containsMember(leader))
            return Majority.leader_lost;
        return Majority.no_change;
    }

    /**
     * Deletes the log data for the given {@link RAFT} instance.
     * <p>
     *     <b>Warning:</b> This should be used in tests only.
     * </p>
     *
     * @param r: RAFT instance to delete the log contents.
     * @throws Exception: If an exception happens while deleting the log.
     * @deprecated Use {@link RaftTestUtils#deleteRaftLog(RAFT)} instead.
     */
    @Deprecated(since = "1.0.13", forRemoval = true)
    public static void deleteLog(RAFT r) throws Exception {
        RaftTestUtils.deleteRaftLog(r);
    }

    /**
     * Extract the Raft ID from the given address instance.
     * <p>
     * During creation, the node embed it's ID in the address. It needs to be an instance of
     * {@link ExtendedUUID} to hold the information.
     * </p>
     *
     * @param address: The {@link Address} to extract the Raft ID.
     * @return The Raft ID embedded in the address, or <code>null</code>, otherwise.
     */
    public static String extractRaftId(Address address) {
        if (!(address instanceof ExtendedUUID))
            return null;

        ExtendedUUID uuid = (ExtendedUUID) address;
        byte[] value = uuid.get(RAFT.raft_id_key);
        return value == null
                ? null
                : Util.bytesToString(value);
    }
}
