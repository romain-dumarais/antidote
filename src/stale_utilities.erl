%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(stale_utilities).

-include("inter_dc_repl.hrl").
-include("antidote.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.


-export([add_pending/3,
         remove_pending/1
        ]).


%%Set a new value to the key.
set(Key, Value, Orddict) ->
    orddict:update(Key, fun(_Old) -> Value end, Value, Orddict).

%%Add pending
add_pending(Dc, CommitTime, PendingVer) ->
    lager:info("Adding pending commit: committime ~w now ~w ~n", [CommitTime, now_microsec(erlang:now())]),
    case orddict:find(Dc, PendingVer) of
        {ok, Q} ->
            Q2 = queue:in(CommitTime, Q),
            set(Dc, Q2, PendingVer);
        error -> %key does not exist
            Q = queue:new(),
            Q2 = queue:in(CommitTime,Q),
            set(Dc, Q2, PendingVer)
    end.


%%Remove pending
remove_pending(StateData)->
    Partition = StateData#recvr_state.partition,
    PendingVer = StateData#recvr_state.pending_ver,
    StaleSum = StateData#recvr_state.stale_sum,
    ProcessedNum = StateData#recvr_state.processed_num,
    CurrentTime = clocksi_vnode:now_microsec(erlang:now()),
    {ok, StableSnapshot} = riak_core_vnode_master:sync_command(
                            {Partition,node()}, get_stable_snapshot,
                             vectorclock_vnode_master),
    {NewPendingVer, NewStaleSum, NewProcessedNum} = remove_pending(PendingVer, StaleSum, ProcessedNum, 
                                                                    StableSnapshot, CurrentTime),
    StateData#recvr_state{pending_ver=NewPendingVer, stale_sum=NewStaleSum, processed_num=NewProcessedNum}.

%%Remove pending. Provide stablesnapshot for test purpose
remove_pending(PendingVer, StaleSum, ProcessedNum, StableSnapshot, CurrentTime) ->
    SnapshotList = dict:to_list(StableSnapshot),
    lists:foldl(fun({Dc, Clock}, {Pending, Sum, Num}) ->
                        case orddict:find(Dc, Pending) of
                            {ok, Queue} ->
                                {NewQ, NewSum, NewNum} = remove_pending_until(Clock, CurrentTime, Queue, Sum, Num),
                                NewPending = set(Dc, NewQ, Pending), 
                                {NewPending, NewSum, NewNum};
                            error ->
                                {Pending, Sum, Num}
                        end end,
                {PendingVer, StaleSum, ProcessedNum}, SnapshotList).

remove_pending_until(Snapshot, CurrentTime, Q, StaleSum, Num) ->
    case queue:is_empty(Q) of
        true ->
            {Q, StaleSum, Num};
        false ->
            E = queue:head(Q),
            case E =< Snapshot of
                true ->
                    lager:info("Removing pending commit: committime ~w currenttime ~w ~n", [E, CurrentTime]),
                    remove_pending_until(Snapshot, CurrentTime, queue:drop(Q),
                        StaleSum+CurrentTime-E, Num+1);
                false ->
                    {Q, StaleSum, Num}
            end
    end.



-ifdef(TEST).

stale_test() ->
    P0 = orddict:new(),
    P1 = add_pending(dc1, 1000, P0),
    ?assertEqual([{dc1, {[1000],[]}}], orddict:to_list(P1)),
    P2 = add_pending(dc1, 2000, P1),
    P3 = add_pending(dc2, 2000, P2),
    ?assertEqual([{dc1, {[2000], [1000]}}, {dc2, {[2000],[]}}], orddict:to_list(P3)),
    P4 = add_pending(dc2, 3000, P3),
    P5 = add_pending(dc3, 500, P4),
    ?assertEqual([{dc1, {[2000], [1000]}}, {dc2, {[3000],[2000]}}, {dc3, {[500], []}}], orddict:to_list(P5)),
    
    Snapshot = dict:new(),
    S1 = dict:store(dc3,600, Snapshot),
    {P6, NewStaleSum, NewProcessedNum} = remove_pending(P5, 0, 0, S1, 1000),
    ?assertEqual({500,1}, {NewStaleSum, NewProcessedNum}),
    
    S2 = dict:store(dc1, 1500, S1),
    {P7, NS1, NP1} = remove_pending(P6, NewStaleSum, NewProcessedNum, S2, 1500),
    ?assertEqual({1000,2}, {NS1, NP1}),

    S3 = dict:store(dc2, 3000, S2),
    {P8, NS2, NP2} = remove_pending(P7, NS1, NP1, S3, 2500),
    ?assertEqual({1000,4}, {NS2, NP2}),

    S4 = dict:update_counter(dc1, 500, S3),
    {_P9, NS3, NP3} = remove_pending(P8, NS2, NP2, S4, 3000),
    ?assertEqual({2000,5}, {NS3, NP3}).
-endif.
