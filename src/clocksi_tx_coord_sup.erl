%% @doc Supervise the fsm.
-module(clocksi_tx_coord_sup).
-behavior(supervisor).

-export([start_fsm/1,
         start_link/0]).
-export([init/1]).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).


start_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).


%% @doc Starts the coordinator of a ClockSI transaction.
init([]) ->
    Worker = {clocksi_tx_coord_fsm,
              {clocksi_tx_coord_fsm, start_link, []},
              transient, 5000, worker, [clocksi_tx_coord_fsm]},
    {ok, {{simple_one_for_one, 5, 10}, [Worker]}}.
