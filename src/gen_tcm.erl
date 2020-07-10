-module(gen_tcm).

-callback newConn(Sock :: gen_tcp:socket(), Args :: term) ->
   ignore |
   {ok, Pid :: pid()} |
   {error, Reason :: term()}.