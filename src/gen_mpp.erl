-module(gen_mpp).

-compile(inline).
-compile({inline_size, 128}).

-include_lib("kernel/include/logger.hrl").
-include("genGbh.hrl").

-import(gen_call, [gcall/3, gcall/4, greply/2, try_greply/2]).

-export([
   %% API for gen_mpp
   start/3, start/4, start_link/3, start_link/4
   , start_monitor/3, start_monitor/4
   , stop/1, stop/3
   , call/2, call/3
   , clfn/4, clfn/5, clfs/4, clfs/5, csfn/4, csfs/4

   , send_request/2, send_request/4
   , wait_response/2, receive_response/2, check_response/2
   , wait_response/3, receive_response/3, check_response/3
   , reqids_new/0, reqids_size/1
   , reqids_add/3, reqids_to_list/1

   , cast/2, send/2, reply/1, reply/2
   , abcast/2, abcast/3
   , multi_call/2, multi_call/3, multi_call/4
   , enter_loop/3, enter_loop/4, enter_loop/5

   %% gen callbacks
   , init_it/6

   %% sys callbacks
   , system_continue/3
   , system_terminate/4
   , system_code_change/4
   , system_get_state/1
   , system_replace_state/2
   , format_status/2

   %% Internal callbacks
   , wakeupFromHib/8

   %% logger callback
   , format_log/1, format_log/2, print_event/3

]).

-define(STACKTRACE(), element(2, erlang:process_info(self(), current_stacktrace))).

-type serverName() ::
   {'local', atom()} |
   {'global', GlobalName :: term()} |
   {'via', RegMod :: module(), Name :: term()}.

-type serverRef() ::
   pid()
   | (LocalName :: atom())
   | {Name :: atom(), Node :: atom()}
   | {'global', GlobalName :: term()}
   | {'via', RegMod :: module(), ViaName :: term()}.

-type startOpt() ::
   daemon |
   {'timeout', Time :: timeout()} |
   {'spawn_opt', [proc_lib:spawn_option()]} |
   enterLoopOpt().

-type enterLoopOpt() ::
   {'debug', Debugs :: [sys:debug_option()]} |
   {'hibernate_after', HibernateAfterTimeout :: timeout()}.

-type startRet() ::
   'ignore' |
   {'ok', pid()} |
   {'ok', {pid(), reference()}} |
   {'error', term()}.

-type action() ::
   timeout() |
   hibernate |
   {'doAfter', Args :: term()}.

-type actions() ::
   action() |
   [action(), ...].

%% gcall 发送消息来源进程格式类型
-type from() :: {To :: pid(), Tag :: term()}.
-type request_id() :: gen:request_id().
-type request_id_collection() :: gen:request_id_collection().
-type response_timeout() :: timeout() | {abs, integer()}.

-type replyAction() ::
{'reply', From :: from(), Reply :: term()}.

-callback init(Args :: term()) ->
   {ok, State :: term()} |
   {ok, State :: term(), Actions :: actions()} |
   {stop, Reason :: term()} |
   ignore.

%% call 消息的返回
-export_type([handleCallRet/0]).
-type handleCallRet() ::
   kpS |
   {reply, Reply :: term()} |
   {reply, Reply :: term(), NewState :: term()} |
   {reply, Reply :: term(), NewState :: term(), Actions :: actions()} |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), Actions :: actions()} |
   {mayReply, Reply :: term()} |
   {mayReply, Reply :: term(), NewState :: term()} |
   {mayReply, Reply :: term(), NewState :: term(), Actions :: actions()} |
   {stop, Reason :: term(), NewState :: term()} |
   {stopReply, Reason :: term(), Reply :: term(), NewState :: term()}.

%% cast 消息的返回
-export_type([handleCastRet/0]).
-type handleCastRet() ::
   kpS |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), Actions :: actions()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback handleInfo(Info :: timeout | term(), State :: term()) ->
   kpS |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), Actions :: actions()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback handleError(Error :: term(), State :: term()) ->
   kpS |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), Actions :: actions()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback handleAfter(Info :: term(), State :: term()) ->
   kpS |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), Actions :: actions()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback terminate(Reason :: (normal | shutdown | {shutdown, term()} |term()), State :: term()) ->
   term().

-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(), Extra :: term()) ->
   {ok, NewState :: term()} | {error, Reason :: term()}.

-callback formatStatus(Opt, StatusData) -> Status when
   Opt :: 'normal' | 'terminate',
   StatusData :: [PDict | State],
   PDict :: [{Key :: term(), Value :: term()}],
   State :: term(),
   Status :: term().

-optional_callbacks([
   handleAfter/2
   , handleInfo/2
   , handleError/2
   , terminate/2
   , code_change/3
   , formatStatus/2
]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% start stop API start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec start(Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start(Module, Args, Options) ->
   gen:start(?MODULE, nolink, Module, Args, Options).

-spec start(ServerName :: serverName(), Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start(ServerName, Module, Args, Options) ->
   gen:start(?MODULE, nolink, ServerName, Module, Args, Options).

-spec start_link(Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start_link(Module, Args, Options) ->
   gen:start(?MODULE, link, Module, Args, Options).

-spec start_link(ServerName :: serverName(), Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start_link(ServerName, Module, Args, Options) ->
   gen:start(?MODULE, link, ServerName, Module, Args, Options).

-spec start_monitor(Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start_monitor(Module, Args, Options) ->
   gen:start(?MODULE, monitor, Module, Args, Options).

-spec start_monitor(ServerName :: serverName(), Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start_monitor(ServerName, Module, Args, Options) ->
   gen:start(?MODULE, monitor, ServerName, Module, Args, Options).

%%停止通用服务器并等待其终止。如果服务器位于另一个节点上，则将监视该节点。
-spec stop(ServerRef :: serverRef()) -> ok.
stop(ServerRef) ->
   gen:stop(ServerRef).

-spec stop(ServerRef :: serverRef(), Reason :: term(), Timeout :: timeout()) -> ok.
stop(ServerRef, Reason, Timeout) ->
   gen:stop(ServerRef, Reason, Timeout).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% start stop API end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% gen callbacks start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
doModuleInit(Module, Args) ->
   try
      Module:init(Args)
   catch
      throw:Ret -> Ret;
      Class:Reason:Strace -> {'EXIT', Class, Reason, Strace}
   end.

init_it(Starter, self, ServerRef, Module, Args, Options) ->
   init_it(Starter, self(), ServerRef, Module, Args, Options);
init_it(Starter, Parent, ServerRef, Module, Args, Options) ->
   Name = gen:name(ServerRef),
   Debug = gen:debug_options(Name, Options),
   GbhOpts = #gbhOpts{daemon = not lists:member(notDaemon, Options)},
   HibernateAfterTimeout = gen:hibernate_after(Options),

   case doModuleInit(Module, Args) of
      {ok, State} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, #{}, State, false);
      {ok, State, Actions} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, #{}, State, listify(Actions));
      {stop, Reason} ->
         % 为了保持一致性，我们必须确保在
         % %%父进程收到有关失败的通知之前，必须先注销%%注册名称（如果有）。
         % %%（否则，如果父进程立即%%再次尝试启动该进程，则其父进程可能会收到％already_started错误）。
         gen:unregister_name(ServerRef),
         proc_lib:init_ack(Starter, {error, Reason}),
         exit(Reason);
      ignore ->
         gen:unregister_name(ServerRef),
         proc_lib:init_ack(Starter, ignore),
         exit(normal);
      {'EXIT', Class, Reason, Stacktrace} ->
         gen:unregister_name(ServerRef),
         proc_lib:init_ack(Starter, {error, terminate_reason(Class, Reason, Stacktrace)}),
         erlang:raise(Class, Reason, Stacktrace);
      _Ret ->
         gen:unregister_name(ServerRef),
         Error = {bad_return_value, _Ret},
         proc_lib:init_ack(Starter, {error, Error}),
         exit(Error)
   end.


%%-----------------------------------------------------------------
%% enter_loop(Module, Options, State, <ServerName>, <TimeOut>) ->_
%%
%% Description: Makes an existing process into a gen_mpp.
%%              The calling process will enter the gen_mpp receive
%%              loop and become a gen_mpp process.
%%              The process *must* have been started using one of the
%%              start functions in proc_lib, see proc_lib(3).
%%              The user is responsible for any initialization of the
%%              process, including registering a name for it.
%%-----------------------------------------------------------------
-spec enter_loop(Module :: module(), State :: term(), Opts :: [enterLoopOpt()]) -> no_return().
enter_loop(Module, State, Opts) ->
   enter_loop(Module, State, Opts, self(), infinity).

-spec enter_loop(Module :: module(), State :: term(), Opts :: [enterLoopOpt()], serverName() | pid()) -> no_return().
enter_loop(Module, State, Opts, ServerName) ->
   enter_loop(Module, State, Opts, ServerName, infinity).

-spec enter_loop(Module :: module(), State :: term(), Opts :: [enterLoopOpt()], Server :: serverName() | pid(), Actions :: actions()) -> no_return().
enter_loop(Module, State, Opts, ServerName, Actions) ->
   Name = gen:get_proc_name(ServerName),
   Parent = gen:get_parent(),
   Debug = gen:debug_options(Name, Opts),
   GbhOpts = #gbhOpts{daemon = not lists:member(notDaemon, Opts)},
   HibernateAfterTimeout = gen:hibernate_after(Opts),
   loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, #{}, State, listify(Actions)).

%%% Internal callbacks
wakeupFromHib(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState) ->
   %% 这是一条新消息，唤醒了我们，因此我们必须立即收到它
   receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, true).

loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Actions) ->
   case doParseAL(Actions, Name, Debug, false, false, Timers) of
      {error, ErrorContent} ->
         innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Actions, error, ErrorContent, ?STACKTRACE());
      {Debug, IsHib, DoAfter, NewTimers} ->
         case DoAfter of
            {doAfter, Args} ->
               doAfterCall(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, NewTimers, CurState, listHib(IsHib), Args);
            _ ->
               case IsHib of
                  true ->
                     proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, NewTimers, CurState]);
                  _ ->
                     receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, NewTimers, CurState, false)
               end
         end
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% sys callbacks start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------
system_continue(Parent, Debug, {Name, Module, GbhOpts, HibernateAfterTimeout, Timers, CurState, IsHib}) ->
   case IsHib of
      true ->
         proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState]);
      _ ->
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false)
   end.

-spec system_terminate(_, _, _, [_]) -> no_return().
system_terminate(Reason, _Parent, Debug, {Name, Module, _GbhOpts, _HibernateAfterTimeout, Timers, CurState, _IsHib}) ->
   terminate(exit, Reason, ?STACKTRACE(), Name, Module, Debug, Timers, CurState, []).

system_code_change({Name, Module, GbhOpts, HibernateAfterTimeout, Timers, CurState, IsHib}, _Module, OldVsn, Extra) ->
   case
      try Module:code_change(OldVsn, CurState, Extra)
      catch
         throw:Result -> Result;
         _C:_R:_S -> {_C, _R, _S}
      end
   of
      {ok, NewState} -> {ok, {Name, Module, GbhOpts, HibernateAfterTimeout, Timers, NewState, IsHib}};
      Error -> Error
   end.

system_get_state({_Name, _Module, _GbhOpts, _HibernateAfterTimeout, _Timers, CurState, _IsHib}) ->
   {ok, CurState}.

system_replace_state(StateFun, {Name, Module, GbhOpts, HibernateAfterTimeout, Timers, CurState, IsHib}) ->
   NewState = StateFun(CurState),
   {ok, NewState, {Name, Module, GbhOpts, HibernateAfterTimeout, Timers, NewState, IsHib}}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% sys callbacks end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API helpers  start  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% -----------------------------------------------------------------
%% Make a call to a generic server.
%% If the server is located at another node, that node will
%% be monitored.
%% If the client is trapping exits and is linked server termination
%% is handled here (? Shall we do that here (or rely on timeouts) ?).
-spec call(ServerRef :: serverRef(), Request :: term()) -> Reply :: term().
call(ServerRef, Request) ->
   try gcall(ServerRef, '$gen_call', Request) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, call, [ServerRef, Request]}}, ?STACKTRACE())
   end.

-spec call(ServerRef :: serverRef(), Request :: term(), Timeout :: timeout()) -> Reply :: term().
call(ServerRef, Request, Timeout) ->
   try gcall(ServerRef, '$gen_call', Request, Timeout) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, call, [ServerRef, Request]}}, ?STACKTRACE())
   end.

-spec clfn(ServerRef :: serverRef(), M :: module(), F :: atom(), A :: list()) -> ok.
clfn(ServerRef, M, F, A) ->
   try gcall(ServerRef, '$gen_clfn', {M, F, A}) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, clfn, [ServerRef, {M, F, A}]}}, ?STACKTRACE())
   end.

-spec clfn(ServerRef :: serverRef(), M :: module(), F :: atom(), A :: list(), Timeout :: timeout()) -> ok.
clfn(ServerRef, M, F, A, Timeout) ->
   try gcall(ServerRef, '$gen_clfn', {M, F, A}, Timeout) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, clfn, [ServerRef, {M, F, A}]}}, ?STACKTRACE())
   end.

-spec clfs(ServerRef :: serverRef(), M :: module(), F :: atom(), A :: list()) -> ok.
clfs(ServerRef, M, F, A) ->
   try gcall(ServerRef, '$gen_clfs', {M, F, A}) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, clfs, [ServerRef, {M, F, A}]}}, ?STACKTRACE())
   end.

-spec clfs(ServerRef :: serverRef(), M :: module(), F :: atom(), A :: list(), Timeout :: timeout()) -> ok.
clfs(ServerRef, M, F, A, Timeout) ->
   try gcall(ServerRef, '$gen_clfs', {M, F, A}, Timeout) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, clfs, [ServerRef, {M, F, A}]}}, ?STACKTRACE())
   end.

-spec csfn(ServerRef :: serverRef(), M :: module(), F :: atom(), A :: list()) -> ok.
csfn({global, Name}, M, F, A) ->
   try global:send(Name, {'$gen_csfn', {M, F, A}}),
   ok
   catch _:_ -> ok
   end;
csfn({via, RegMod, Name}, M, F, A) ->
   try RegMod:send(Name, {'$gen_csfn', {M, F, A}}),
   ok
   catch _:_ -> ok
   end;
csfn(Dest, M, F, A) ->
   try erlang:send(Dest, {'$gen_csfn', {M, F, A}}),
   ok
   catch _:_ -> ok
   end.

-spec csfs(ServerRef :: serverRef(), M :: module(), F :: atom(), A :: list()) -> ok.
csfs({global, Name}, M, F, A) ->
   try global:send(Name, {'$gen_csfs', {M, F, A}}),
   ok
   catch _:_ -> ok
   end;
csfs({via, RegMod, Name}, M, F, A) ->
   try RegMod:send(Name, {'$gen_csfs', {M, F, A}}),
   ok
   catch _:_ -> ok
   end;
csfs(Dest, M, F, A) ->
   try erlang:send(Dest, {'$gen_csfs', {M, F, A}}),
   ok
   catch _:_ -> ok
   end.

%%% -----------------------------------------------------------------
%%% Make a call to servers at several nodes.
%%% Returns: {[Replies],[BadNodes]}
%%% A Timeout can be given
%%%
%%% A middleman process is used in case late answers arrives after
%%% the timeout. If they would be allowed to glog the callers message
%%% queue, it would probably become confused. Late answers will
%%% now arrive to the terminated middleman and so be discarded.
%%% -----------------------------------------------------------------

-spec multi_call(
   Name    :: atom(),
   Request :: term()
) ->
   {Replies ::
   [{Node :: node(), Reply :: term()}],
      BadNodes :: [node()]
   }.
%%
multi_call(Name, Request)
   when is_atom(Name) ->
   multi_call([node() | nodes()], Name, Request, infinity).

-spec multi_call(
   Nodes   :: [node()],
   Name    :: atom(),
   Request :: term()
) ->
   {Replies ::
   [{Node :: node(), Reply :: term()}],
      BadNodes :: [node()]
   }.
%%
multi_call(Nodes, Name, Request)
   when is_list(Nodes), is_atom(Name) ->
   multi_call(Nodes, Name, Request, infinity).

-spec multi_call(
   Nodes   :: [node()],
   Name    :: atom(),
   Request :: term(),
   Timeout :: timeout()
) ->
   {Replies ::
   [{Node :: node(), Reply :: term()}],
      BadNodes :: [node()]
   }.
-define(is_timeout(X), ( (X) =:= infinity orelse ( is_integer(X) andalso (X) >= 0 ) )).
multi_call(Nodes, Name, Request, Timeout)
   when is_list(Nodes), is_atom(Name), ?is_timeout(Timeout) ->
   Alias = alias(),
   try
      Timer = if Timeout == infinity -> undefined;
                 true -> erlang:start_timer(Timeout, self(), Alias)
              end,
      Reqs = mc_send(Nodes, Name, Alias, Request, Timer, []),
      mc_recv(Reqs, Alias, Timer, [], [])
   after
      _ = unalias(Alias)
   end.

-dialyzer({no_improper_lists, mc_send/6}).

mc_send([], _Name, _Alias, _Request, _Timer, Reqs) ->
   Reqs;
mc_send([Node|Nodes], Name, Alias, Request, Timer, Reqs) when is_atom(Node) ->
   NN = {Name, Node},
   Mon = try
            erlang:monitor(process, NN, [{tag, Alias}])
         catch
            error:badarg ->
               %% Node not alive...
               M = make_ref(),
               Alias ! {Alias, M, process, NN, noconnection},
               M
         end,
   try
      %% We use 'noconnect' since it is no point in bringing up a new
      %% connection if it was not brought up by the monitor signal...
      _ = erlang:send(NN,
         {'$gen_call', {self(), [[alias|Alias]|Mon]}, Request},
         [noconnect]),
      ok
   catch
      _:_ ->
         ok
   end,
   mc_send(Nodes, Name, Alias, Request, Timer, [[Node|Mon]|Reqs]);
mc_send(_BadNodes, _Name, Alias, _Request, Timer, Reqs) ->
   %% Cleanup then fail...
   unalias(Alias),
   mc_cancel_timer(Timer, Alias),
   _ = mc_recv_tmo(Reqs, Alias, [], []),
   error(badarg).

mc_recv([], Alias, Timer, Replies, BadNodes) ->
   mc_cancel_timer(Timer, Alias),
   unalias(Alias),
   {Replies, BadNodes};
mc_recv([[Node|Mon] | RestReqs] = Reqs, Alias, Timer, Replies, BadNodes) ->
   receive
      {[[alias|Alias]|Mon], Reply} ->
         erlang:demonitor(Mon, [flush]),
         mc_recv(RestReqs, Alias, Timer, [{Node,Reply}|Replies], BadNodes);
      {Alias, Mon, process, _, _} ->
         mc_recv(RestReqs, Alias, Timer, Replies, [Node|BadNodes]);
      {timeout, Timer, Alias} ->
         unalias(Alias),
         mc_recv_tmo(Reqs, Alias, Replies, BadNodes)
   end.

mc_recv_tmo([], _Alias, Replies, BadNodes) ->
   {Replies, BadNodes};
mc_recv_tmo([[Node|Mon] | RestReqs], Alias, Replies, BadNodes) ->
   erlang:demonitor(Mon),
   receive
      {[[alias|Alias]|Mon], Reply} ->
         mc_recv_tmo(RestReqs, Alias, [{Node,Reply}|Replies], BadNodes);
      {Alias, Mon, process, _, _} ->
         mc_recv_tmo(RestReqs, Alias, Replies, [Node|BadNodes])
   after
      0 ->
         mc_recv_tmo(RestReqs, Alias, Replies, [Node|BadNodes])
   end.

mc_cancel_timer(undefined, _Alias) ->
   ok;
mc_cancel_timer(Timer, Alias) ->
   case erlang:cancel_timer(Timer) of
      false ->
         receive
            {timeout, Timer, Alias} ->
               ok
         end;
      _ ->
         ok
   end.

-spec cast(ServerRef :: serverRef(), Msg :: term()) -> ok.
cast({global, Name}, Msg) ->
   try global:send(Name, {'$gen_cast', Msg}),
   ok
   catch _:_ -> ok
   end;
cast({via, RegMod, Name}, Msg) ->
   try RegMod:send(Name, {'$gen_cast', Msg}),
   ok
   catch _:_ -> ok
   end;
cast(Dest, Msg) ->
   try erlang:send(Dest, {'$gen_cast', Msg}),
   ok
   catch _:_ -> ok
   end.

-spec send(ServerRef :: serverRef(), Msg :: term()) -> ok.
send({global, Name}, Msg) ->
   try global:send(Name, Msg),
   ok
   catch _:_ -> ok
   end;
send({via, RegMod, Name}, Msg) ->
   try RegMod:send(Name, Msg),
   ok
   catch _:_ -> ok
   end;
send(Dest, Msg) ->
   try erlang:send(Dest, Msg),
   ok
   catch _:_ -> ok
   end.


%% 异步广播，不返回任何内容，只是发送“ n”祈祷
abcast(Name, Msg) when is_atom(Name) ->
   doAbcast([node() | nodes()], Name, Msg).

abcast(Nodes, Name, Msg) when is_list(Nodes), is_atom(Name) ->
   doAbcast(Nodes, Name, Msg).

doAbcast(Nodes, Name, Msg) ->
   [
      begin
         try erlang:send({Name, Node}, {'$gen_cast', Msg}),
         ok
         catch
            _:_ -> ok
         end
      end || Node <- Nodes
   ],
   ok.

%% Reply from a status machine callback to whom awaits in call/2
-spec reply([replyAction(), ...] | replyAction()) -> ok.
reply({reply, {To, Tag}, Reply}) ->
   try To ! {Tag, Reply},
   ok
   catch _:_ ->
      ok
   end;
reply(Replies) when is_list(Replies) ->
   [greply(From, Reply) || {reply, From, Reply} <- Replies],
   ok.

-compile({inline, [reply/2]}).
-spec reply(From :: from(), Reply :: term()) -> ok.
reply(From, Reply) ->
   greply(From, Reply).

%% -----------------------------------------------------------------
%% Send a request to a generic server and return a Key which should be
%% used with wait_response/2 or check_response/2 to fetch the
%% result of the request.

-spec send_request(ServerRef::serverRef(), Request::term()) ->
   ReqId::request_id().

send_request(ServerRef, Request) ->
   try
      gen:send_request(ServerRef, '$gen_call', Request)
   catch
      error:badarg ->
         error(badarg, [ServerRef, Request])
   end.

-spec send_request(ServerRef::serverRef(),
   Request::term(),
   Label::term(),
   ReqIdCollection::request_id_collection()) ->
   NewReqIdCollection::request_id_collection().

send_request(ServerRef, Request, Label, ReqIdCol) ->
   try
      gen:send_request(ServerRef, '$gen_call', Request, Label, ReqIdCol)
   catch
      error:badarg ->
         error(badarg, [ServerRef, Request, Label, ReqIdCol])
   end.

-spec wait_response(ReqId, WaitTime) -> Result when
   ReqId :: request_id(),
   WaitTime :: response_timeout(),
   Response :: {reply, Reply::term()}
   | {error, {Reason::term(), serverRef()}},
   Result :: Response | 'timeout'.

wait_response(ReqId, WaitTime) ->
   try
      gen:wait_response(ReqId, WaitTime)
   catch
      error:badarg ->
         error(badarg, [ReqId, WaitTime])
   end.

-spec wait_response(ReqIdCollection, WaitTime, Delete) -> Result when
   ReqIdCollection :: request_id_collection(),
   WaitTime :: response_timeout(),
   Delete :: boolean(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef()}},
   Result :: {Response,
      Label::term(),
      NewReqIdCollection::request_id_collection()} |
   'no_request' |
   'timeout'.

wait_response(ReqIdCol, WaitTime, Delete) ->
   try
      gen:wait_response(ReqIdCol, WaitTime, Delete)
   catch
      error:badarg ->
         error(badarg, [ReqIdCol, WaitTime, Delete])
   end.

-spec receive_response(ReqId, Timeout) -> Result when
   ReqId :: request_id(),
   Timeout :: response_timeout(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef()}},
   Result :: Response | 'timeout'.

receive_response(ReqId, Timeout) ->
   try
      gen:receive_response(ReqId, Timeout)
   catch
      error:badarg ->
         error(badarg, [ReqId, Timeout])
   end.

-spec receive_response(ReqIdCollection, Timeout, Delete) -> Result when
   ReqIdCollection :: request_id_collection(),
   Timeout :: response_timeout(),
   Delete :: boolean(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef()}},
   Result :: {Response,
      Label::term(),
      NewReqIdCollection::request_id_collection()} |
   'no_request' |
   'timeout'.

receive_response(ReqIdCol, Timeout, Delete) ->
   try
      gen:receive_response(ReqIdCol, Timeout, Delete)
   catch
      error:badarg ->
         error(badarg, [ReqIdCol, Timeout, Delete])
   end.

-spec check_response(Msg, ReqId) -> Result when
   Msg :: term(),
   ReqId :: request_id(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef()}},
   Result :: Response | 'no_reply'.

check_response(Msg, ReqId) ->
   try
      gen:check_response(Msg, ReqId)
   catch
      error:badarg ->
         error(badarg, [Msg, ReqId])
   end.

-spec check_response(Msg, ReqIdCollection, Delete) -> Result when
   Msg :: term(),
   ReqIdCollection :: request_id_collection(),
   Delete :: boolean(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef()}},
   Result :: {Response,
      Label::term(),
      NewReqIdCollection::request_id_collection()} |
   'no_request' |
   'no_reply'.

check_response(Msg, ReqIdCol, Delete) ->
   try
      gen:check_response(Msg, ReqIdCol, Delete)
   catch
      error:badarg ->
         error(badarg, [Msg, ReqIdCol, Delete])
   end.

-spec reqids_new() ->
   NewReqIdCollection::request_id_collection().

reqids_new() ->
   gen:reqids_new().

-spec reqids_size(ReqIdCollection::request_id_collection()) ->
   non_neg_integer().

reqids_size(ReqIdCollection) ->
   try
      gen:reqids_size(ReqIdCollection)
   catch
      error:badarg -> error(badarg, [ReqIdCollection])
   end.

-spec reqids_add(ReqId::request_id(), Label::term(),
   ReqIdCollection::request_id_collection()) ->
   NewReqIdCollection::request_id_collection().

reqids_add(ReqId, Label, ReqIdCollection) ->
   try
      gen:reqids_add(ReqId, Label, ReqIdCollection)
   catch
      error:badarg -> error(badarg, [ReqId, Label, ReqIdCollection])
   end.

-spec reqids_to_list(ReqIdCollection::request_id_collection()) ->
   [{ReqId::request_id(), Label::term()}].

reqids_to_list(ReqIdCollection) ->
   try
      gen:reqids_to_list(ReqIdCollection)
   catch
      error:badarg -> error(badarg, [ReqIdCollection])
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API helpers  end  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, IsHib) ->
   receive
      Msg ->
         case Msg of
            {'$gen_call', From, Request} ->
               matchCallMsg(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, From, Request);
            {'$gen_cast', Cast} ->
               matchCastMsg(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Cast);
            {'$gen_clfn', From, MFA} ->
               matchMFA(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, From, MFA, false);
            {'$gen_clfs', From, MFA} ->
               matchMFA(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, From, MFA, true);
            {'$gen_csfn', MFA} ->
               matchMFA(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false, MFA, false);
            {'$gen_csfs', MFA} ->
               matchMFA(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false, MFA, true);
            {system, PidFrom, Request} ->
               %% 不返回但尾递归调用 system_continue/3
               sys:handle_system_msg(Request, PidFrom, Parent, ?MODULE, Debug, {Name, Module, GbhOpts, HibernateAfterTimeout, Timers, CurState, IsHib}, IsHib);
            {'EXIT', Parent, Reason} ->
               terminate(exit, Reason, ?STACKTRACE(), Name, Module, Debug, Timers, CurState, Msg);
            _ ->
               matchInfoMsg(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Msg)
         end
   after HibernateAfterTimeout ->
      proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState])
   end.

matchCallMsg(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, From, Request) ->
   ?SYS_DEBUG(Debug, Name, {in, {{call, From}, Request}}),
   try
      case is_tuple(Request) of
         true ->
            FunName = element(1, Request),
            Module:FunName(Request, CurState, From);
         _ ->
            Module:Request(Request, CurState, From)
      end
   of
      Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, From, false)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, From, false);
      error:undef ->
         try_greply(From, {error, undef}),
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
      Class:Reason:Strace ->
         try_greply(From, {error, {inner_error, {Class, Reason, Strace}}}),
         innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, {{call, From}, Request}, Class, Reason, Strace)
   end.

matchCastMsg(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Cast) ->
   ?SYS_DEBUG(Debug, Name, {in, {cast, Cast}}),
   try
      case is_tuple(Cast) of
         true ->
            FunName = element(1, Cast),
            Module:FunName(Cast, CurState);
         _ ->
            Module:Cast(Cast, CurState)
      end
   of
      Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, false, false)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, false, false);
      error:undef ->
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
      Class:Reason:Strace ->
         innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, {cast, Cast}, Class, Reason, Strace)
   end.

matchMFA(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, From, MFA, IsWithState) ->
   ?SYS_DEBUG(Debug, Name, {in, {mfa, MFA}}),
   try
      {M, F, A} = MFA,
      case IsWithState of
         true ->
            erlang:apply(M, F, A ++ [CurState]);
         _ ->
            erlang:apply(M, F, A)
      end
   of
      Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, From, true)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, From, true);
      error:undef ->
         try_greply(From, {error, undef}),
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
      Class:Reason:Strace ->
         try_greply(From, {error, {inner_error, {Class, Reason, Strace}}}),
         innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, {mfa, MFA}, Class, Reason, Strace)
   end.

matchInfoMsg(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Msg) ->
   ?SYS_DEBUG(Debug, Name, {in, {info, Msg}}),
   try Module:handleInfo(Msg, CurState) of
      Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, false, false)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, false, false);
      Class:Reason:Strace ->
         innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, {info, Msg}, Class, Reason, Strace)
   end.

doAfterCall(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, LeftAction, Args) ->
   ?SYS_DEBUG(Debug, Name, {in, {doAfter, Args}}),
   try Module:handleAfter(Args, CurState) of
      Result ->
         handleAR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, LeftAction, Result)
   catch
      throw:Result ->
         handleAR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, LeftAction, Result);
      Class:Reason:Strace ->
         innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, {doAfter, Args}, Class, Reason, Strace)
   end.

handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, From, IsAnyRet) ->
   case Result of
      kpS ->
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
      {reply, Reply} ->
         reply(From, Reply),
         ?SYS_DEBUG(Debug, Name, {out, Reply, From, CurState}),
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
      {mayReply, Reply} ->
         case From of
            false ->
               receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
            _ ->
               greply(From, Reply),
               ?SYS_DEBUG(Debug, Name, {out, Reply, From, CurState}),
               receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false)
         end;
      {noreply, NewState} ->
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, false);
      {reply, Reply, NewState} ->
         reply(From, Reply),
         ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
         receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, false);
      {mayReply, Reply, NewState} ->
         case From of
            false ->
               receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, false);
            _ ->
               greply(From, Reply),
               ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
               receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, false)
         end;
      {noreply, NewState, Actions} ->
         loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, listify(Actions));
      {stop, Reason, NewState} ->
         terminate(exit, Reason, ?STACKTRACE(), Name, Module, Debug, Timers, NewState, {return, stop});
      {reply, Reply, NewState, Actions} ->
         reply(From, Reply),
         ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
         loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, listify(Actions));
      {mayReply, Reply, NewState, Actions} ->
         case From of
            false ->
               loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, listify(Actions));
            _ ->

               greply(From, Reply),
               ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
               loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, listify(Actions))
         end;
      {stopReply, Reason, Reply, NewState} ->
         ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
         try
            terminate(exit, Reason, ?STACKTRACE(), Name, Module, Debug, Timers, NewState, {return, stop_reply})
         after
            _ = reply(From, Reply)
         end;
      _AnyRet ->
         case IsAnyRet of
            true ->
               receiveIng(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, false);
            _ ->
               innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, {return, _AnyRet}, error, bad_ret, ?STACKTRACE())
         end
   end.

handleAR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, LeftAction, Result) ->
   case Result of
      kpS ->
         loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, LeftAction);
      {noreply, NewState} ->
         loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, LeftAction);
      {noreply, NewState, Actions} ->
         loopEntry(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, NewState, listify(Actions) ++ LeftAction);
      {stop, Reason, NewState} ->
         terminate(exit, Reason, ?STACKTRACE(), Name, Module, Debug, Timers, NewState, {return, stop_reply})
   end.

%% loopParseActionsList
doParseAL([], _Name, Debug, IsHib, DoAfter, Timers) ->
   {Debug, IsHib, DoAfter, Timers};
doParseAL([OneAction | LeftActions], Name, Debug, IsHib, DoAfter, Timers) ->
   case OneAction of
      hibernate ->
         doParseAL(LeftActions, Name, Debug, true, DoAfter, Timers);
      {'doAfter', _Args} ->
         doParseAL(LeftActions, Name, Debug, IsHib, OneAction, Timers);
      infinity ->
         doParseAL(LeftActions, Name, Debug, IsHib, DoAfter, Timers);
      Timeout when is_integer(Timeout) ->
         erlang:send_after(Timeout, self(), timeout),
         ?SYS_DEBUG(Debug, Name, {start_timer, {timeout, Timeout, timeout, []}}),
         doParseAL(LeftActions, Name, Debug, IsHib, DoAfter, Timers);
      _ ->
         {error, {bad_ActionType, OneAction}}
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% timer deal start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
listify(Item) when is_list(Item) ->
   Item;
listify(Item) ->
   [Item].

listHib(false) ->
   [];
listHib(_) ->
   [hibernate].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% timer deal end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
innerError(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, MsgEvent, Class, Reason, Stacktrace) ->
   case GbhOpts of
      #gbhOpts{daemon = true} ->
         error_msg({innerError, {Class, Reason, Stacktrace}}, Name, undefined, MsgEvent, Module, Debug, CurState),
         case erlang:function_exported(Module, handleError, 2) of
            true ->
               try Module:handleError({Class, Reason, Stacktrace}, CurState) of
                  Result ->
                     handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, false, false)
               catch
                  throw:Result ->
                     handleCR(Parent, Name, Module, GbhOpts, HibernateAfterTimeout, Debug, Timers, CurState, Result, false, false);
                  IClass:IReason:IStrace ->
                     error_msg({handleError, {IClass, IReason, IStrace}}, Name, undefined, {Class, Reason, Stacktrace}, Module, Debug, CurState),
                     kpS
               end;
            false ->
               kpS
         end;
      _ ->
         terminate(Class, Reason, Stacktrace, Name, Module, Debug, Timers, CurState, MsgEvent)
   end.

%%% ---------------------------------------------------
%%% Terminate the server.
%%%
%%% terminate/8 is triggered by {stop, Reason} or bad
%%% return values. The stacktrace is generated via the
%%% ?STACKTRACE() macro and the ReportReason must not
%%% be wrapped in tuples.
%%%
%%% terminate/9 is triggered in case of error/exit in
%%% the user callback. In this case the report reason
%%% always includes the user stacktrace.
%%%
%%% The reason received in the terminate/2 callbacks
%%% always includes the stacktrace for errors and never
%%% for exits.
%%% ---------------------------------------------------
terminate(Class, Reason, Stacktrace, Name, Module, Debug, _Timers, CurState, MsgEvent) ->
   Reply = try_terminate(Module, terminate_reason(Class, Reason, Stacktrace), CurState),
   case Reply of
      {'EXIT', C, R, S} ->
         error_info({R, S}, Name, undefined, MsgEvent, Module, Debug, CurState),
         erlang:raise(C, R, S);
      _ ->
         case {Class, Reason} of
            {exit, normal} -> ok;
            {exit, shutdown} -> ok;
            {exit, {shutdown, _}} -> ok;
            _ ->
               error_info(Reason, Name, undefined, MsgEvent, Module, Debug, CurState)
         end
   end,
   case Stacktrace of
      [] ->
         erlang:Class(Reason);
      _ ->
         erlang:raise(Class, Reason, Stacktrace)
   end.

try_terminate(Mod, Reason, State) ->
   case erlang:function_exported(Mod, terminate, 2) of
      true ->
         try
            {ok, Mod:terminate(Reason, State)}
         catch
            throw:Ret ->
               {ok, Ret};
            Class:Reason:Strace ->
               {'EXIT', Class, Reason, Strace}
         end;
      false ->
         {ok, ok}
   end.

terminate_reason(error, Reason, Stacktrace) -> {Reason, Stacktrace};
terminate_reason(exit, Reason, _Stacktrace) -> Reason.

error_info(_Reason, application_controller, _From, _Msg, _Mod, _State, _Debug) ->
   %% OTP-5811 Don't send an error report if it's the system process
   %% application_controller which is terminating - let init take care
   %% of it instead
   ok;
error_info(Reason, Name, From, Msg, Module, Debug, State) ->
   Log = sys:get_log(Debug),
   ?LOG_ERROR(#{label => {gen_mpp, terminate},
      name => Name,
      last_message => Msg,
      state => format_status(terminate, Module, get(), State),
      log => format_log_state(Module, Log),
      reason => Reason,
      client_info => client_stacktrace(From)},
      #{
         domain => [otp],
         report_cb => fun gen_mpp:format_log/2,
         error_logger => #{tag => error, report_cb => fun gen_mpp:format_log/1}
      }),
   ok.

error_msg(Reason, Name, From, Msg, Module, Debug, State) ->
   Log = sys:get_log(Debug),
   ?LOG_ERROR(#{label => {gen_mpp, inner_error},
      name => Name,
      last_message => Msg,
      state => format_status(inner_error, Module, get(), State),
      log => format_log_state(Module, Log),
      reason => Reason,
      client_info => client_stacktrace(From)},
      #{
         domain => [otp],
         report_cb => fun gen_mpp:format_log/2,
         error_logger => #{tag => error, report_cb => fun gen_mpp:format_log/1}
      }),
   ok.

client_stacktrace(undefined) ->
   undefined;
client_stacktrace({From, _Tag}) ->
   client_stacktrace(From);
client_stacktrace(From) when is_pid(From), node(From) =:= node() ->
   case process_info(From, [current_stacktrace, registered_name]) of
      undefined ->
         {From, dead};
      [{current_stacktrace, Stacktrace}, {registered_name, []}] ->
         {From, {From, Stacktrace}};
      [{current_stacktrace, Stacktrace}, {registered_name, Name}] ->
         {From, {Name, Stacktrace}}
   end;
client_stacktrace(From) when is_pid(From) ->
   {From, remote}.


%% format_log/1 is the report callback used by Logger handler
%% error_logger only. It is kept for backwards compatibility with
%% legacy error_logger event handlers. This function must always
%% return {Format,Args} compatible with the arguments in this module's
%% calls to error_logger prior to OTP-21.0.
format_log(Report) ->
   Depth = error_logger:get_format_depth(),
   FormatOpts = #{chars_limit => unlimited,
      depth => Depth,
      single_line => false,
      encoding => utf8},
   format_log_multi(limit_report(Report, Depth), FormatOpts).

limit_report(Report, unlimited) ->
   Report;
limit_report(#{label := {gen_mpp, terminate},
   last_message := Msg,
   state := State,
   log := Log,
   reason := Reason,
   client_info := Client} = Report,
   Depth) ->
   Report#{
      last_message => io_lib:limit_term(Msg, Depth),
      state => io_lib:limit_term(State, Depth),
      log => [io_lib:limit_term(L, Depth) || L <- Log],
      reason => io_lib:limit_term(Reason, Depth),
      client_info => limit_client_report(Client, Depth)
   };
limit_report(#{label := {gen_mpp, inner_error},
   last_message := Msg,
   state := State,
   log := Log,
   reason := Reason,
   client_info := Client} = Report,
   Depth) ->
   Report#{
      last_message => io_lib:limit_term(Msg, Depth),
      state => io_lib:limit_term(State, Depth),
      log => [io_lib:limit_term(L, Depth) || L <- Log],
      reason => io_lib:limit_term(Reason, Depth),
      client_info => limit_client_report(Client, Depth)
   }.

limit_client_report({From, {Name, Stacktrace}}, Depth) ->
   {From, {Name, io_lib:limit_term(Stacktrace, Depth)}};
limit_client_report(Client, _) ->
   Client.

%% format_log/2 is the report callback for any Logger handler, except
%% error_logger.
format_log(Report, FormatOpts0) ->
   Default = #{chars_limit => unlimited,
      depth => unlimited,
      single_line => false,
      encoding => utf8},
   FormatOpts = maps:merge(Default, FormatOpts0),
   IoOpts =
      case FormatOpts of
         #{chars_limit := unlimited} ->
            [];
         #{chars_limit := Limit} ->
            [{chars_limit, Limit}]
      end,
   {Format, Args} = format_log_single(Report, FormatOpts),
   io_lib:format(Format, Args, IoOpts).

format_log_single(#{label := {gen_mpp, terminate},
   name := Name,
   last_message := Msg,
   state := State,
   log := Log,
   reason := Reason,
   client_info := Client},
   #{single_line := true, depth := Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Format1 = lists:append(["Generic server ", P, " terminating. Reason: ", P,
      ". Last message: ", P, ". State: ", P, "."]),
   {ServerLogFormat, ServerLogArgs} = format_server_log_single(Log, FormatOpts),
   {ClientLogFormat, ClientLogArgs} = format_client_log_single(Client, FormatOpts),

   Args1 =
      case Depth of
         unlimited ->
            [Name, fix_reason(Reason), Msg, State];
         _ ->
            [Name, Depth, fix_reason(Reason), Depth, Msg, Depth, State, Depth]
      end,
   {Format1 ++ ServerLogFormat ++ ClientLogFormat, Args1 ++ ServerLogArgs ++ ClientLogArgs};
format_log_single(#{label := {gen_mpp, inner_error},
   name := Name,
   last_message := Msg,
   state := State,
   log := Log,
   reason := Reason,
   client_info := Client},
   #{single_line := true, depth := Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Format1 = lists:append(["Generic server ", P, " terminating. Reason: ", P,
      ". Last message: ", P, ". State: ", P, "."]),
   {ServerLogFormat, ServerLogArgs} = format_server_log_single(Log, FormatOpts),
   {ClientLogFormat, ClientLogArgs} = format_client_log_single(Client, FormatOpts),

   Args1 =
      case Depth of
         unlimited ->
            [Name, fix_reason(Reason), Msg, State];
         _ ->
            [Name, Depth, fix_reason(Reason), Depth, Msg, Depth, State, Depth]
      end,
   {Format1 ++ ServerLogFormat ++ ClientLogFormat, Args1 ++ ServerLogArgs ++ ClientLogArgs};
format_log_single(Report, FormatOpts) ->
   format_log_multi(Report, FormatOpts).

format_log_multi(#{label := {gen_mpp, terminate},
   name := Name,
   last_message := Msg,
   state := State,
   log := Log,
   reason := Reason,
   client_info := Client},
   #{depth := Depth} = FormatOpts) ->
   Reason1 = fix_reason(Reason),
   {ClientFmt, ClientArgs} = format_client_log(Client, FormatOpts),
   P = p(FormatOpts),
   Format =
      lists:append(
         ["** gen_mpp ", P, " terminating \n"
         "** Last message in was ", P, "~n"
         "** When Server state == ", P, "~n"
         "** Reason for termination ==~n** ", P, "~n"] ++
         case Log of
            [] -> [];
            _ -> ["** Log ==~n** [" |
               lists:join(",~n    ", lists:duplicate(length(Log), P))] ++
            ["]~n"]
         end) ++ ClientFmt,
   Args =
      case Depth of
         unlimited ->
            [Name, Msg, State, Reason1] ++ Log ++ ClientArgs;
         _ ->
            [Name, Depth, Msg, Depth, State, Depth, Reason1, Depth] ++
               case Log of
                  [] -> [];
                  _ -> lists:flatmap(fun(L) -> [L, Depth] end, Log)
               end ++ ClientArgs
      end,
   {Format, Args};
format_log_multi(#{label := {gen_mpp, inner_error},
   name := Name,
   last_message := Msg,
   state := State,
   log := Log,
   reason := Reason,
   client_info := Client},
   #{depth := Depth} = FormatOpts) ->
   Reason1 = fix_reason(Reason),
   {ClientFmt, ClientArgs} = format_client_log(Client, FormatOpts),
   P = p(FormatOpts),
   Format =
      lists:append(
         ["** gen_mpp ", P, " inner_error \n"
         "** Last message in was ", P, "~n"
         "** When Server state == ", P, "~n"
         "** Reason for termination ==~n** ", P, "~n"] ++
         case Log of
            [] -> [];
            _ -> ["** Log ==~n** [" |
               lists:join(",~n    ", lists:duplicate(length(Log), P))] ++
            ["]~n"]
         end) ++ ClientFmt,
   Args =
      case Depth of
         unlimited ->
            [Name, Msg, State, Reason1] ++ Log ++ ClientArgs;
         _ ->
            [Name, Depth, Msg, Depth, State, Depth, Reason1, Depth] ++
               case Log of
                  [] -> [];
                  _ -> lists:flatmap(fun(L) -> [L, Depth] end, Log)
               end ++ ClientArgs
      end,
   {Format, Args}.

fix_reason({undef, [{M, F, A, L} | MFAs]} = Reason) ->
   case code:is_loaded(M) of
      false ->
         {'module could not be loaded', [{M, F, A, L} | MFAs]};
      _ ->
         case erlang:function_exported(M, F, length(A)) of
            true ->
               Reason;
            false ->
               {'function not exported', [{M, F, A, L} | MFAs]}
         end
   end;
fix_reason(Reason) ->
   Reason.

format_server_log_single([], _) ->
   {"", []};
format_server_log_single(Log, FormatOpts) ->
   Args =
      case maps:get(depth, FormatOpts) of
         unlimited ->
            [Log];
         Depth ->
            [Log, Depth]
      end,
   {" Log: " ++ p(FormatOpts), Args}.

format_client_log_single(undefined, _) ->
   {"", []};
format_client_log_single({From, dead}, _) ->
   {" Client ~0p is dead.", [From]};
format_client_log_single({From, remote}, _) ->
   {" Client ~0p is remote on node ~0p.", [From, node(From)]};
format_client_log_single({_From, {Name, Stacktrace0}}, FormatOpts) ->
   P = p(FormatOpts),
   %% Minimize the stacktrace a bit for single line reports. This is
   %% hopefully enough to point out the position.
   Stacktrace = lists:sublist(Stacktrace0, 4),
   Args =
      case maps:get(depth, FormatOpts) of
         unlimited ->
            [Name, Stacktrace];
         Depth ->
            [Name, Depth, Stacktrace, Depth]
      end,
   {" Client " ++ P ++ " stacktrace: " ++ P ++ ".", Args}.

format_client_log(undefined, _) ->
   {"", []};
format_client_log({From, dead}, _) ->
   {"** Client ~p is dead~n", [From]};
format_client_log({From, remote}, _) ->
   {"** Client ~p is remote on node ~p~n", [From, node(From)]};
format_client_log({_From, {Name, Stacktrace}}, FormatOpts) ->
   P = p(FormatOpts),
   Format = lists:append(["** Client ", P, " stacktrace~n",
      "** ", P, "~n"]),
   Args =
      case maps:get(depth, FormatOpts) of
         unlimited ->
            [Name, Stacktrace];
         Depth ->
            [Name, Depth, Stacktrace, Depth]
      end,
   {Format, Args}.

p(#{single_line := Single, depth := Depth, encoding := Enc}) ->
   "~" ++ single(Single) ++ mod(Enc) ++ p(Depth);
p(unlimited) ->
   "p";
p(_Depth) ->
   "P".

single(true) -> "0";
single(false) -> "".

mod(latin1) -> "";
mod(_) -> "t".

%%-----------------------------------------------------------------
%% Status information
%%-----------------------------------------------------------------
format_status(Opt, StatusData) ->
   [PDict, SysState, Parent, Debug, {Name, Module, _GbhOpts, _HibernateAfterTimeout, _Timers, CurState, _IsHib}] = StatusData,
   Header = gen:format_status_header("Status for generic server", Name),
   Log = sys:get_log(Debug),
   Specific =
      case format_status(Opt, Module, PDict, CurState) of
         S when is_list(S) -> S;
         S -> [S]
      end,
   [{header, Header},
      {data, [{"Status", SysState},
         {"Parent", Parent},
         {"Logged events", format_log_state(Module, Log)}]} |
      Specific].

format_log_state(Module, Log) ->
   [case Event of
       {out, Msg, From, State} ->
          {out, Msg, From, format_status(terminate, Module, get(), State)};
       {noreply, State} ->
          {noreply, format_status(terminate, Module, get(), State)};
       _ -> Event
    end || Event <- Log].

format_status(Opt, Module, PDict, State) ->
   DefStatus =
      case Opt of
         terminate -> State;
         _ -> [{data, [{"State", State}]}]
      end,
   case erlang:function_exported(Module, format_status, 2) of
      true ->
         case catch Module:formatStatus(Opt, [PDict, State]) of
            {'EXIT', _} -> DefStatus;
            Else -> Else
         end;
      _ ->
         DefStatus
   end.

%%-----------------------------------------------------------------
%% Format debug messages.  Print them as the call-back module sees
%% them, not as the real erlang messages.  Use trace for that.
%%-----------------------------------------------------------------
print_event(Dev, {in, Msg}, Name) ->
   case Msg of
      {{call, {From, _Tag}}, Call} ->
         io:format(Dev, "*DBG* ~tp got call ~tp from ~tw~n",
            [Name, Call, From]);
      {cast, Cast} ->
         io:format(Dev, "*DBG* ~tp got cast ~tp~n",
            [Name, Cast]);
      _ ->
         io:format(Dev, "*DBG* ~tp got ~tp~n", [Name, Msg])
   end;
print_event(Dev, {out, Msg, {To, _Tag}, State}, Name) ->
   io:format(Dev, "*DBG* ~tp sent ~tp to ~tw, new state ~tp~n",
      [Name, Msg, To, State]);
print_event(Dev, {noreply, State}, Name) ->
   io:format(Dev, "*DBG* ~tp new state ~tp~n", [Name, State]);
print_event(Dev, Event, Name) ->
   io:format(Dev, "*DBG* ~tp dbg  ~tp~n", [Name, Event]).