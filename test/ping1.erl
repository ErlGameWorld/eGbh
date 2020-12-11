-module(ping1).

-compile([export_all, nowarn_export_all]).

call(N) ->
   {ok, Pid} = pong1:start(),
   StartTime = erlang:system_time(nanosecond),
   doCall(N, Pid, StartTime).

doCall(0, Pid, StartTime) ->
   EndTime = erlang:system_time(nanosecond),
   exit(Pid, kill),
   io:format("call1 over use time: ~p ns~n", [EndTime - StartTime]);
doCall(N, Pid, StartTime) ->
   gen_server:call(Pid, ping),
   doCall(N - 1, Pid, StartTime).

