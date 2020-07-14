-module(ping2).

-compile([export_all, nowarn_export_all]).

call(N) ->
   {ok, Pid} = pong2:start(),
   StartTime = erlang:system_time(nanosecond),
   doCall(N, Pid, StartTime).

doCall(0, Pid, StartTime) ->
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("call2 over use time: ~p ns~n",[EndTime - StartTime]);
doCall(N, Pid, StartTime) ->
   gen_srv:call(Pid, ping),
   doCall(N - 1, Pid, StartTime).

send(N) ->
   {ok, Pid} = pong2:start(),
   StartTime = erlang:system_time(nanosecond),
   doSend(N, Pid, StartTime).

doSend(0, Pid, StartTime) ->
  % Ret = gen_srv:call(Pid, ping),
   Ret = 1,
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("send2 over use time: ~p ~p ns~n",[Ret, EndTime - StartTime]);
doSend(N, Pid, StartTime) ->
   gen_srv:send(Pid, ping),
   doSend(N - 1, Pid, StartTime).

cast(N) ->
   {ok, Pid} = pong2:start(),
   StartTime = erlang:system_time(nanosecond),
   doCast(N, Pid, StartTime).

doCast(0, Pid, StartTime) ->
   %Ret = gen_srv:call(Pid, ping),
   Ret = 1,
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("cast2 over use time: ~p ~p ns~n",[Ret, EndTime - StartTime]);
doCast(N, Pid, StartTime) ->
   gen_srv:cast(Pid, ping),
   doCast(N - 1, Pid, StartTime).
