-module(pong1).

-behavior(gen_server).

-compile([export_all, nowarn_export_all]).

start() ->
   gen_server:start(?MODULE, 0, []).

init(_Args)  ->
   {ok, 0}.

handle_call(ping, _From, State) ->
   {reply, pong, State}.

handle_cast(_Msg, State) ->
   {noreply, State}.