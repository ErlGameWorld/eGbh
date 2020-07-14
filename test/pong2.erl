-module(pong2).

-behavior(gen_srv).

-compile([export_all, nowarn_export_all]).

start() ->
   gen_srv:start(?MODULE, 0, []).

init(_Args)  ->
   {ok, 0}.

handleCall(ping, _State, _From) ->
   {reply, pong}.

handleCast(_Msg, State) ->
   {noreply, State}.

handleInfo(_Msg, State) ->
   {noreply, State}.