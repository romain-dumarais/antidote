%% -------------------------------------------------------------------
%%
%% Copyright (c) 2014 SyncFree Consortium.  All Rights Reserved.
%%
% This file is provided to you under the Apache License,
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
-module(oppolicy_test).

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").
-define(HARNESS, (rt_config:get(rt_harness))).

confirm() ->
    NumVNodes = rt_config:get(num_vnodes, 8),
    rt:update_app_config(all,[
        {riak_core, [{ring_creation_size, NumVNodes}]}
    ]),

    Clean = rt_config:get(clean_cluster, true),
    [Cluster1, Cluster2] = rt:build_clusters([1,1]),
    rt:wait_until_ring_converged(Cluster1),
    rt:wait_until_ring_converged(Cluster2),

    ok = common:setup_dc_manager([Cluster1, Cluster2], first_run),

    empty_policy_test(Cluster1, Cluster2),

    [Cluster3, Cluster4] = common:clean_clusters([Cluster1, Cluster2]),
    ok = common:setup_dc_manager([Cluster3, Cluster4], Clean),
    concurrent_set_retain_test(Cluster3, Cluster4),

    % [Nodes2] = common:clean_clusters([Nodes1]),
    % remove_test(Nodes2),
    pass.


empty_policy_test(Cluster1, _Cluster2) ->
    FirstNode = hd(Cluster1),
    lager:info("Empty policy test started"),
    Type = crdt_policy,
    Key = key_empty,
    Result0=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, []}, Result0),
    Result3=rpc:call(FirstNode, antidote, append,
                    [Key, Type, {{set_right, [read]}, actor1}]),
    ?assertMatch({ok, _}, Result3),
    Result4=rpc:call(FirstNode, antidote, read,
                    [Key, Type]),
    ?assertMatch({ok, [read]}, Result4).

concurrent_set_retain_test(Cluster1, Cluster2) ->
    FirstNode = hd(Cluster1),
    SecondNode = hd(Cluster2),
    lager:info("Check if concurrent set operations are retained"),
    Type = crdt_policy,
    Key = key_set_retain,

    % partition FirstNode from SecondNode
    {ok, D1} = rpc:call(FirstNode, inter_dc_manager, get_descriptor, []),

    ok = rpc:call(SecondNode, inter_dc_manager, forget_dcs, [[D1]]),

    % write different policies on the two nodes
    Result0=rpc:call(FirstNode, antidote, append,
                      [Key, Type, {{set_right, [read]}, actor1}]),
    ?assertMatch({ok, _}, Result0),
    {ok, {_, _, CommitTime0}} = Result0,
    ?assertMatch({ok, [read]}, rpc:call(FirstNode, antidote, read,
                                            [Key, Type])),
    Result1=rpc:call(SecondNode, antidote, append,
                      [Key, Type, {{set_right, [read, write]}, actor2}]),
    ?assertMatch({ok, _}, Result1),
    {ok, {_, _, CommitTime1}} = Result1,
    ?assertMatch({ok, [read, write]}, rpc:call(SecondNode, antidote, read,
                                                  [Key, Type])),

    % heal the cluster again and converge
    [ok] = rpc:call(SecondNode, inter_dc_manager, observe_dcs, [[D1]]),

    % check the convergence of the policy values (retaining both)
    CombinedTime = vectorclock:max([CommitTime0, CommitTime1]),
    Result2=rpc:call(FirstNode, antidote, clocksi_read,
                      [CombinedTime, Key, Type]),
    ?assertMatch({ok, _}, Result2),
    {ok, {_, [Value], _}} = Result2,
    ?assertMatch([read], Value).


% add_test(Nodes) ->
%     FirstNode = hd(Nodes),
%     lager:info("Add test started"),
%     Type = crdt_orset,
%     Key = key_add,
%     %%Add multiple key works
%     Result0=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{add, a}, ucl}}}, {update, {Key, Type, {{add, b}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result0),
%     Result1=rpc:call(FirstNode, antidote, read,
%                     [Key, Type]),
%     ?assertMatch({ok, [a,b]}, Result1),
%     %%Add a key twice in a transaction only adds one
%     Result2=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{add, c}, ucl}}}, {update, {Key, Type, {{add, c}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result2),
%     Result3=rpc:call(FirstNode, antidote, read,
%                     [Key, Type]),
%     ?assertMatch({ok, [a,b,c]}, Result3),
%     %%Add a key multiple time will not duplicate
%     Result4=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{add, a}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result4),
%     Result5=rpc:call(FirstNode, antidote, read,
%                     [Key, Type]),
%     ?assertMatch({ok, [a,b,c]}, Result5).
%
%
% remove_test(Nodes) ->
%     FirstNode = hd(Nodes),
%     lager:info("Remove started"),
%     Type = crdt_orset,
%     Key = key_remove,
%
%     Result1=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{add, a}, ucl}}}, {update, {Key, Type, {{add, b}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result1),
%     Result2=rpc:call(FirstNode, antidote, read,
%                     [Key, Type]),
%     ?assertMatch({ok, [a,b]}, Result2),
%     %% Remove an element works
%     Result3=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{remove, a}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result3),
%     Result4=rpc:call(FirstNode, antidote, read,
%                     [Key, Type]),
%     ?assertMatch({ok, [b]}, Result4),
%     %%Add back and remove all works
%     Result5=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{add, a}, ucl}}}, {update, {Key, Type, {{add, b}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result5),
%     %%Remove all
%     Result6=rpc:call(FirstNode, antidote, clocksi_execute_tx,
%                     [[{update, {Key, Type, {{remove, a}, ucl}}}, {update, {Key, Type, {{remove, b}, ucl}}}]]),
%     ?assertMatch({ok, _}, Result6),
%     Result7=rpc:call(FirstNode, antidote, read,
%                     [Key, Type]),
%     ?assertMatch({ok, []}, Result7).
