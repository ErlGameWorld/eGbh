-module(ping2).

-compile([export_all]).

ping(N) ->
   {ok, Pid} = pong2:start(),
   StartTime = erlang:system_time(nanosecond),
   doPing(N, Pid, StartTime).

doPing(0, _Pid, StartTime) ->
   EndTime = erlang:system_time(nanosecond),
   io:format("ping2 over use time: ~p ns~n",[EndTime - StartTime]);
doPing(N, Pid, StartTime) ->
   gen_srv:call(Pid, ping),
   doPing(N - 1, Pid, StartTime).