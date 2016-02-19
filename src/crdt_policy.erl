%% -------------------------------------------------------------------
%%
%% crdt_policy: A convergent, replicated, operation based security policy implementation
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
%% An operation-based Security Policy implementation
%% As the data structure is operation-based, to issue an operation, one should
%% firstly call `generate_downstream/3' to get the downstream version of the
%% operation and then call `update/2'.
%%
%% It provides an operation set_right to update the right. Concurrent updates
%% lead to multiple versions of the policy. When querying the policy for the
%% current value with get_right, the minimum of the concurrently assigned rights
%% is returned.
%%
%% In the current implementation, the rights are modeled as sets of arbitrary
%% elements. In the most basic version, sets of atoms are used, where each atom
%% represents an operation which may be performed. The minimum is computed as
%% the intersection of these sets.
%%
%% @end
-module(crdt_policy).

%% API
-export([new/0, value/1, generate_downstream/3, update/2, equal/2,
    to_binary/1, from_binary/1, value/2, is_operation/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export_type([policy/0, right/0, binary_policy/0, policy_op/0, policy_downstream_op/0]).
-opaque policy() :: orddict:orddict().

-type binary_policy() :: binary(). % A binary that from_binary/1 will operation on

-type policy_op() :: {set_right, right()}.

%% cannot be opaque because dialyzer does not like this for the generate_downstream spec
-type policy_downstream_op() :: {set_right, right(), binary(), policy()}.

-type right() :: ordsets:ordset(term()).

-type actor() :: riak_dt:actor().

-spec new() -> policy().
new() ->
    orddict:new().


-spec value(policy()) -> [right()].
value(Policy) ->
    orddict:fold(fun(Right, Tokens, Res)->
                  lists:duplicate(length(Tokens), Right) ++ Res end,
                 [], Policy).

-spec value(get_right, policy()) -> right().
value(get_right, Policy) ->
    case orddict:fetch_keys(Policy) of
        [] ->
            ordsets:new();
        [Head|Tail] ->
            lists:foldl(fun ordsets:intersection/2, Head, Tail)
    end.

-spec generate_downstream(policy_op(), actor(), policy()) -> {ok, policy_downstream_op()}.
generate_downstream({set_right, Right}, Actor, CurrentPolicy) ->
    Token = unique(Actor),
    {ok, {set_right, Right, Token, CurrentPolicy}}.

-spec update(policy_downstream_op(), policy()) -> {ok, policy()}.
update({set_right, Right, Token, Dependencies}, Policy) ->
    add_right(Right, Token, remove_rights(Dependencies, Policy)).

-spec equal(policy(), policy()) -> boolean().
equal(Policy1, Policy2) ->
    Policy1 == Policy2. % Should work if comparing orddicts works


%-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, 99). % this should be part of riak_dt
-define(V1_VERS, 1).

-spec to_binary(policy()) -> binary_policy().
to_binary(Policy) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Policy))/binary>>.

-spec from_binary(binary_policy()) -> {ok, policy()}.
from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

-spec is_operation(term()) -> boolean().
is_operation(Operation) ->
    case Operation of
        {set_right, _Right} ->
            true;
        _ ->
            false
    end.


% Private
%-spec add_right(right(), binary(), policy()) -> {ok, policy()}.
add_right(Right, Token, Policy) ->
    case orddict:find(Right, Policy) of
        {ok, Tokens} ->
            case lists:member(Token, Tokens) of
                true ->
                    {ok, Policy};
                false ->
                    {ok, orddict:store(Right, Tokens++[Token], Policy)}
            end;
        error ->
            {ok, orddict:store(Right, [Token], Policy)}
    end.

%-spec remove_right(right(), [binary()], policy()) -> policy().
remove_right(Right, RemoveTokens, Policy) ->
    case orddict:find(Right, Policy) of
        {ok, Tokens} ->
            RestTokens = Tokens--RemoveTokens,
            case RestTokens of
                [] ->
                    orddict:erase(Right, Policy);
                _ ->
                    orddict:store(Right, RestTokens, Policy)
            end;
        error ->
            Policy % if the right is not in the policy anymore, it was already removed by a different policy change
    end.

%-spec remove_rights(orddict:orddict(), policy()) -> policy().
remove_rights(Orddict, Policy) ->
    orddict:fold(fun(Right, Tokens, Res) ->
                   remove_right(Right, Tokens, Res)
                 end, Policy, Orddict).

%-spec unique(actor()) -> binary().
unique(_Actor) ->
    crypto:strong_rand_bytes(20).


%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).
new_test() ->
    ?assertEqual(orddict:new(), new()).

set_test() ->
    Policy1 = new(),
    {ok, DownstreamOp1} = generate_downstream({set_right, ordsets:from_list([read])}, 1, Policy1),
    ?assertMatch({set_right, [read], _, _}, DownstreamOp1),
    {ok, DownstreamOp2} = generate_downstream({set_right, ordsets:from_list([read, write])}, 1, Policy1),
    ?assertMatch({set_right, [read, write], _, _}, DownstreamOp2),
    {ok, Policy2} = update(DownstreamOp1, Policy1),
    {_, Right1, _, _} = DownstreamOp1,
    ?assertEqual([Right1], orddict:fetch_keys(Policy2)),
    {ok, Policy3} = update(DownstreamOp2, Policy1),
    {_, Right2, _, _} = DownstreamOp2,
    ?assertEqual([Right2], orddict:fetch_keys(Policy3)).

value_test() ->
    Policy1 = new(),
    ?assertEqual([], value(Policy1)),

    {ok, DownstreamOp1} = generate_downstream({set_right, ordsets:from_list([read])}, 1, Policy1),
    {ok, Policy2} = update(DownstreamOp1, Policy1),
    ?assertEqual([[read]], value(Policy2)),
    ?assertEqual([read], value(get_right, Policy2)),

    {ok, DownstreamOp2} = generate_downstream({set_right, ordsets:from_list([read, write])}, 1, Policy2),
    {ok, Policy3} = update(DownstreamOp2, Policy2),
    ?assertEqual([[read, write]], value(Policy3)),
    ?assertEqual([read, write], value(get_right, Policy3)).

concurrent_set_retain_test() ->
    Policy1 = new(),
    %% Set one version of the policy
    {ok, Op1} = generate_downstream({set_right, ordsets:from_list([read])}, 1, Policy1),
    {ok, Policy2} = update(Op1, Policy1),
    ?assertEqual([[read]], value(Policy2)),

    %% If a second set if concurrent to the first one, retain both policies
    {ok, Op2} = generate_downstream({set_right, ordsets:from_list([read, write])}, 1, Policy1),
    {ok, Policy3} = update(Op2, Policy2),
    Values3 = value(Policy3),
    ?assert(lists:member([read, write], Values3)),
    ?assert(lists:member([read], Values3)),

    %% Return the minimum as the current right
    ?assertEqual([read], value(get_right, Policy3)).

concurrent_update_test() ->
    Policy1 = new(),
    %% Create a policy with multiple concurrent versions
    {ok, Op1} = generate_downstream({set_right, ordsets:from_list([read])}, 1, Policy1),
    {ok, Policy2} = update(Op1, Policy1),
    {ok, Op2} = generate_downstream({set_right, ordsets:from_list([read, write])}, 1, Policy1),
    {ok, PolicyMult} = update(Op2, Policy2),
    ?assertEqual(2, length(value(PolicyMult))),

    %% Update the policy on the replica only seeing the [read] right (Policy2)
    {ok, Op3} = generate_downstream({set_right, ordsets:from_list([read, write, own])}, 1, Policy2),
    %% Apply the operation to the multi-version state
    {ok, PolicyUpdated} = update(Op3, PolicyMult),
    ValuesUpdated = value(PolicyUpdated),
    ?assertEqual(2, length(ValuesUpdated)),
    ?assert(lists:member(ordsets:from_list([read, write]), ValuesUpdated)),
    ?assert(lists:member(ordsets:from_list([read, write, own]), ValuesUpdated)),

    %% Return the minimum as the current right
    ?assertEqual([read, write], value(get_right, PolicyUpdated)).

minimum_test() ->
    Policy1 = new(),
    %% Create policy with overlapping rights
    {ok, Op1} = generate_downstream({set_right, ordsets:from_list([read, write])}, 1, Policy1),
    {ok, Policy2} = update(Op1, Policy1),
    {ok, Op2} = generate_downstream({set_right, ordsets:from_list([write, own])}, 1, Policy1),
    {ok, PolicyMult} = update(Op2, Policy2),

    %% The minimum should be the intersection of the sets
    ?assertEqual([write], value(get_right, PolicyMult)).

binary_test() ->
    Policy1 = new(),
    BinaryPolicy1 = to_binary(Policy1),
    Policy2 = from_binary(BinaryPolicy1),
    ?assert(equal(Policy1, Policy2)),

    {ok, Op1} = generate_downstream({set_right, [read, write]}, 1, Policy1),
    {ok, Policy3} = update(Op1, Policy1),
    BinaryPolicy3 = to_binary(Policy3),
    Policy4 = from_binary(BinaryPolicy3),
    ?assert(equal(Policy3, Policy4)).

-endif.
