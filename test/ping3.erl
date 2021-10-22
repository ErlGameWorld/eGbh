-module(ping3).

-compile([export_all, nowarn_export_all]).

call(N) ->
   {ok, Pid} = pong3:start(),
   StartTime = erlang:system_time(nanosecond),
   doCall(N, Pid, StartTime).

doCall(0, Pid, StartTime) ->
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("call3 over use time: ~p ns~n", [EndTime - StartTime]);
doCall(N, Pid, StartTime) ->
   gen_apu:call(Pid, mPing),
   doCall(N - 1, Pid, StartTime).

send(N) ->
   {ok, Pid} = pong3:start(),
   StartTime = erlang:system_time(nanosecond),
   doSend(N, Pid, StartTime).

doSend(0, Pid, StartTime) ->
   % Ret = gen_apu:call(Pid, ping),
   Ret = 1,
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("send2 over use time: ~p ~p ns~n", [Ret, EndTime - StartTime]);
doSend(N, Pid, StartTime) ->
   gen_apu:send(Pid, mPing),
   doSend(N - 1, Pid, StartTime).

cast(N) ->
   {ok, Pid} = pong3:start(),
   StartTime = erlang:system_time(nanosecond),
   doCast(N, Pid, StartTime).

doCast(0, Pid, StartTime) ->
   %Ret = gen_apu:call(Pid, ping),
   Ret = 1,
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("cast3 over use time: ~p ~p ns~n", [Ret, EndTime - StartTime]);
doCast(N, Pid, StartTime) ->
   gen_apu:cast(Pid, mPing),
   doCast(N - 1, Pid, StartTime).
