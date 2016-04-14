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
    to_binary/1, from_binary/1, value/2, is_operation/1]).

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
    dict:map(fun({Key, Type}, Value) ->
      Type:value(Value)
    end, Map).

% -spec generate_downstream(policy_op(), actor(), policy()) -> {ok, policy_downstream_op()}.
generate_downstream({update, {Key, Type}, Op}, Actor, CurrentMap) ->
    % TODO could be optimized for some types
    CurrentValue = case is_key({Key, Type}, CurrentMap) of
      true -> dict:fetch({Key, Type}, CurrentMap);
      false -> Type:new()
    end,
    {ok, {update, {Key, Type}, Type:generate_downstream(Op, Actor, CurrentValue)}};

-spec update(policy_downstream_op(), policy()) -> {ok, policy()}.
update({update, {Key, Type}, Op}, Map) ->
    case is_key({Key, Type}, Map) of
      true -> dict:update({Key, Type}, fun(V) -> Type:update(Op, V) end, Map);
      false -> NewValue = Type:new(),
               Type:update(Op, NewValue),
               dict:append({Key, Type}, NewValue, Map)
    end.

-spec equal(policy(), policy()) -> boolean().
equal(Map1, Map2) ->
    Map1 == Map2. % TODO better implementation


%-include_lib("riak_dt/include/riak_dt_tags.hrl").
-define(TAG, 101). % this should be part of riak_dt
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
        {update, {_Key, _Type}, _Op} ->
            true;
        _ ->
            false
    end.


%% ===================================================================
%% EUnit tests
%% ===================================================================
% TODO
