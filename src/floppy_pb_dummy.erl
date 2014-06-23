-module(floppy_pb_dummy).

-ifdef(TEST).
-compile([export_all]).
-include_lib("eunit/include/eunit.hrl").
-endif.

-behaviour(riak_api_pb_service).

-include_lib("riak_pb/include/floppy_pb.hrl").

-export([init/0,
         decode/2,
         encode/1,
         process/2,
         process_stream/3
        ]).

-record(state, {client}).  % local client

%% @doc init/0 callback. Returns the service internal start
%% state.
init() ->
    #state{}.

%% @doc decode/2 callback. Decodes an incoming message.
decode(Code, Bin) ->
    Msg = riak_pb_codec:decode(Code, Bin),
    case Msg of
        #fpbincrementreq{} ->
            {ok, Msg, {"floppy.inc", <<>>}};
        #fpbdecrementreq{} ->
            {ok, Msg, {"floppy.dec", <<>>}};
        #fpbgetcounterreq{} ->
            {ok, Msg, {"floppy.getcounter", <<>>}}
    end.

%% @doc encode/1 callback. Encodes an outgoing response message.
encode(Message) ->
    {ok, riak_pb_codec:encode(Message)}.

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbincrementreq{key=Key, amount=Amount}, State) ->
    {ok,_Result} = floppy:append(Key,{increment,Amount}),
    lager:info("processing increment on Key ~p, with amount ~p",[Key,Amount]),
    {reply, #fpboperationresp{success = true}, State};

%% @doc process/2 callback. Handles an incoming request message.
process(#fpbdecrementreq{key=Key, amount=Amount}, State) ->
    {ok,_Result} = floppy:append(Key,{decrement,Amount}),
    {reply, #fpboperationresp{success = true}, State};

%% @doc process/2 callback. Handles an incoming request message.
%% @todo accept different types of counters.
process(#fpbgetcounterreq{key=Key}, State) ->
    Result = floppy:read(Key,riak_dt_pncounter),
    {reply, #fpbgetcounterresp{value = Result}, State}.

%% @doc process_stream/3 callback. This service does not create any
%% streaming responses and so ignores all incoming messages.
process_stream(_,_,State) ->
    {ignore, State}.


