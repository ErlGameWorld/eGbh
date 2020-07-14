-module(ping3).

-compile([export_all]).

send(N) ->
   {ok, Pid} = pong2:start(),
   StartTime = erlang:system_time(nanosecond),
   doSend(N, Pid, StartTime).

doSend(0, Pid, StartTime) ->
   Ret = gen_srv:call(Pid, ping),
   EndTime = erlang:system_time(nanosecond),
   io:format("ping2 over use time: ~p ~p ns~n",[Ret, EndTime - StartTime]);
doSend(N, Pid, StartTime) ->
   gen_srv:send(Pid, ping),
   doSend(N - 1, Pid, StartTime).

cast(N) ->
   {ok, Pid} = pong2:start(),
   StartTime = erlang:system_time(nanosecond),
   doCast(N, Pid, StartTime).

doCast(0, Pid, StartTime) ->
   Ret = gen_srv:call(Pid, ping),
   EndTime = erlang:system_time(nanosecond),
   io:format("ping2 over use time: ~p ~p ns~n",[Ret, EndTime - StartTime]);
doCast(N, Pid, StartTime) ->
   gen_srv:cast(Pid, ping),
   doCast(N - 1, Pid, StartTime).