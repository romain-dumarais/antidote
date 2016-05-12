%% -------------------------------------------------------------------
%%
%% crdt_map:TODO
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

%% @doc
%% TODO
%%
%% @end
-module(crdt_map).

%% API
-export([new/0, value/1, generate_downstream/3, update/2, equal/2,
    to_binary/1, from_binary/1, is_operation/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

% -export_type([policy/0, right/0, binary_policy/0, policy_op/0, policy_downstream_op/0]).
% -opaque policy() :: orddict:orddict().
%
% -type binary_policy() :: binary(). % A binary that from_binary/1 will operation on
%
% -type policy_op() :: {set_right, right()}.
%
% %% cannot be opaque because dialyzer does not like this for the generate_downstream spec
% -type policy_downstream_op() :: {set_right, right(), binary(), policy()}.
%
% -type right() :: ordsets:ordset(term()).
%
% -type actor() :: riak_dt:actor().

% -spec new() -> policy().
new() ->
    dict:new().

% -spec value(policy()) -> right().
value(Map) ->
    dict:map(fun({_Key, Type}, Value) ->
      Type:value(Value)
    end, Map).

% -spec generate_downstream(policy_op(), actor(), policy()) -> {ok, policy_downstream_op()}.
generate_downstream({update, {{Key, Type}, Op}}, Actor, CurrentMap) ->
    % TODO could be optimized for some types
    CurrentValue = case dict:is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> Type:new()
    end,
    {ok, DownstreamOp} = Type:generate_downstream(Op, Actor, CurrentValue),
    {ok, {update, {{Key, Type}, DownstreamOp}}};
generate_downstream({update, Ops}, Actor, CurrentMap) when is_list(Ops) ->
    {ok, {update, lists:map(fun(Op) -> {ok, DSOp} = generate_downstream(Op, Actor, CurrentMap), DSOp end, Ops)}}.

update({update, {{Key, Type}, Op}}, Map) ->
    case dict:is_key({Key, Type}, Map) of
      true -> {ok, dict:update({Key, Type}, fun(V) -> {ok, Value} = Type:update(Op, V), Value end, Map)};
      false -> NewValue = Type:new(),
               {ok, NewValueUpdated} = Type:update(Op, NewValue),
               {ok, dict:store({Key, Type}, NewValueUpdated, Map)}
    end;
update({update, Ops}, Map) ->
    apply_ops(Ops, Map).

apply_ops([], Map) ->
    {ok, Map};
apply_ops([Op | Rest], Map) ->
    case update(Op, Map) of
        {ok, ORDict1} -> apply_ops(Rest, ORDict1);
        Error -> Error
    end.

equal(Map1, Map2) ->
    Map1 == Map2. % TODO better implementation


%-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, 101). % this should be part of riak_dt
-define(V1_VERS, 1).

to_binary(Policy) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Policy))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

is_operation(Operation) ->
    case Operation of
        {update, {{_Key, _Type}, _Op}} ->
            true;
        {update, Ops} when is_list(Ops) ->
            lists:all(fun is_operation/1, Ops);
        _ ->
            false
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
% TODO
-ifdef(TEST).
new_test() ->
    ?assertEqual(dict:new(), new()).

update_test() ->
    Map1 = new(),
    {ok, DownstreamOp} = generate_downstream({update, {{key1, crdt_lwwreg}, {assign, <<"test">>}}}, actor, Map1),
    ?assertMatch({update, {{key1, crdt_lwwreg}, {assign, <<"test">>, _TS}}}, DownstreamOp),
    {ok, Map2} = update(DownstreamOp, Map1),
    Key1Value = dict:fetch({key1, crdt_lwwreg}, value(Map2)),
    ?assertEqual(<<"test">>, Key1Value).
-endif.
