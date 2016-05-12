%% -------------------------------------------------------------------
%%
%% crdt_lwwreg: an antidote implementation of a last-writer-wins register
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
-module(crdt_lwwreg).

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
  {<<>>, 0}.

% -spec value(policy()) -> right().
value({Value, _TS}) ->
  Value.

% -spec generate_downstream(policy_op(), actor(), policy()) -> {ok, policy_downstream_op()}.
generate_downstream({assign, Value}, _Actor, _OldLWWReg) ->
  {ok, {assign, Value, make_micro_epoch()}}.

make_micro_epoch() ->
    {Mega, Sec, Micro} = os:timestamp(),
    (Mega * 1000000 + Sec) * 1000000 + Micro.

% taken from the riak_dt implementation
update({assign, Value, TS}, {_OldValue, OldTS}) when TS > OldTS ->
  {ok, {Value, TS}};
update({assign, _Value, TS}, {OldValue, OldTS}) when TS < OldTS ->
  {ok, {OldValue, OldTS}};
update({assign, Value, TS}, {OldValue, _OldTS}) when Value >= OldValue ->
  {ok, {Value, TS}};
update({assign, _Value, _TS}, OldLWWReg) ->
  {ok, OldLWWReg}.

equal(Value1, Value2) ->
    Value1 == Value2.


%-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, 102). % this should be part of riak_dt
-define(V1_VERS, 1).

to_binary(Value) ->
    %% @TODO something smarter
    <<?TAG:8/integer, ?V1_VERS:8/integer, (term_to_binary(Value))/binary>>.

from_binary(<<?TAG:8/integer, ?V1_VERS:8/integer, Bin/binary>>) ->
    %% @TODO something smarter
    binary_to_term(Bin).

is_operation(Operation) ->
    case Operation of
        {assign, _Value} ->
            true;
        _ ->
            false
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
% TODO
-ifdef(TEST).
new_test() ->
  ?assertEqual({<<>>, 0}, new()).

assign_test() ->
  Reg1 = new(),
  {ok, DownstreamOp} = generate_downstream({assign, <<"value1">>}, actor, Reg1),
  ?assertMatch({assign, <<"value1">>, _TS}, DownstreamOp),
  {ok, Reg2} = update(DownstreamOp, Reg1),
  ?assertEqual(<<"value1">>, value(Reg2)).

concurrent_assign_test() ->
  Reg1 = new(),

  % assign new value "value1"
  {ok, Op1} = generate_downstream({assign, <<"value1">>}, actor, Reg1),
  {ok, Reg2} = update(Op1, Reg1),
  ?assertEqual(<<"value1">>, value(Reg2)),

  % concurrently assign value "value2"
  {ok, Op2} = generate_downstream({assign, <<"value2">>}, actor, Reg1),
  {ok, Reg3} = update(Op2, Reg1),
  ?assertEqual(<<"value2">>, value(Reg3)),

  % apply concurrent updates
  {ok, Reg4} = update(Op2, Reg2),
  {ok, Reg5} = update(Op1, Reg3),

  % resulting value should be the same
  ?assertEqual(value(Reg4), value(Reg5)).
-endif.
