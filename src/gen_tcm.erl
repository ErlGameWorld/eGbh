-module(gen_tcm).

-callback newConnect(Sock :: gen_tcp:socket(), Args :: term) ->
   ignore |
   {ok, Pid :: pid()} |
   {error, Reason :: term()}.