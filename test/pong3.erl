-module(pong3).

-behavior(gen_apu).

-compile([export_all, nowarn_export_all]).

start() ->
   gen_apu:start(?MODULE, 0, []).

init(_Args) ->
   {ok, 0}.

mPing(_State, _From) ->
   {reply, pong}.

mPing(_State) ->
   kpS.