-record(recvr_state,
        {lastRecvd :: orddict:orddict(), %TODO: this may not be required
         lastCommitted :: orddict:orddict(),
         %%Track timestamps from other DC which have been committed by this DC
         recQ :: orddict:orddict(), %% Holds recieving updates from each DC separately in causal order.
         statestore,
         pending_ver :: orddict:orddict(), %%Pending versions that are not yet visible to local DC
         stale_sum :: integer(), %%The sum of staleness count
         processed_num :: non_neg_integer(), %%The number of object versions processed and visible to local DC 
         partition}).
