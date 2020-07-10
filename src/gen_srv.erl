-module(gen_srv).

-compile(inline).
-compile({inline_size, 128}).

-include_lib("kernel/include/logger.hrl").

-export([
   %% API for gen_srv
   start/3, start/4, start_link/3, start_link/4
   , start_monitor/3, start_monitor/4
   , stop/1, stop/3
   , call/2, call/3
   , send_request/2, wait_response/2, check_response/2
   , cast/2, reply/2
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
   , format_log/1, format_log/2

]).

-define(STACKTRACE(), element(2, erlang:process_info(self(), current_stacktrace))).

%% debug 调试相关宏定义
-define(NOT_DEBUG, []).
-define(SYS_DEBUG(Debug, Name, Msg),
   case Debug of
      ?NOT_DEBUG ->
         Debug;
      _ ->
         sys:handle_debug(Debug, fun print_event/3, Name, Msg)
   end).


-callback init(Args :: term()) ->
   {ok, State :: term()} |
   {ok, State :: term(), action()} |
   {stop, Reason :: term()} |
   ignore.


-callback handleCall(Request :: term(), State :: term(), From :: {pid(), Tag :: term()}) ->
   ok |
   {reply, Reply :: term(), NewState :: term()} |
   {reply, Reply :: term(), NewState :: term(), action() | action()} |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), action() | action()} |
   {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback handleCast(Request :: term(), State :: term()) ->
   ok |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), action() | action()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback handleInfo(Info :: timeout | term(), State :: term()) ->
   ok |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), action() | action()} |
   {stop, Reason :: term(), NewState :: term()}.

-callback handleAfter(Info :: term(), State :: term()) ->
   ok |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), action() | action()} |
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
   handleCall/3
   , handleCast/2
   , handleInfo/2
   , handleAfter/2
   , terminate/2
   , code_change/3
   , formatStatus/2
]).

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

-type requestId() :: term().

-type startOpt() ::
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
   {'doAfter', Args :: term()} |
   timeoutAction().

-type timeoutAction() ::
   timeoutNewAction() |
   timeoutCancelAction() |
   timeoutUpdateAction().

-type timeoutNewAction() ::
   {'nTimeout', Name :: term(), Time :: timeouts(), TimeoutMsg :: term()} |
   {'nTimeout', Name :: term(), Time :: timeouts(), TimeoutMsg :: term(), Options :: ([timeoutOption()])}.

-type timeoutUpdateAction() ::
   {'uTimeout', Name :: term(), TimeoutMsg :: term()}.

-type timeoutCancelAction() ::
   {'cTimeout', Name :: term()}.

-type timeouts() :: Time :: timeout() | integer().
-type timeoutOption() :: {abs, Abs :: boolean()}.

%% -type timer() :: #{TimeoutName :: atom() => {TimerRef :: reference(), TimeoutMsg :: term()}}.


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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API helpers  start  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% -----------------------------------------------------------------
%% Make a call to a generic server.
%% If the server is located at another node, that node will
%% be monitored.
%% If the client is trapping exits and is linked server termination
%% is handled here (? Shall we do that here (or rely on timeouts) ?).
%% -----------------------------------------------------------------
call(Name, Request) ->
   case catch gen:call(Name, '$gen_call', Request) of
      {ok, Res} ->
         Res;
      {'EXIT', Reason} ->
         exit({Reason, {?MODULE, call, [Name, Request]}})
   end.

call(Name, Request, Timeout) ->
   case catch gen:call(Name, '$gen_call', Request, Timeout) of
      {ok, Res} ->
         Res;
      {'EXIT', Reason} ->
         exit({Reason, {?MODULE, call, [Name, Request, Timeout]}})
   end.

%% -----------------------------------------------------------------
%% Send a request to a generic server and return a Key which should be
%% used with wait_response/2 or check_response/2 to fetch the
%% result of the request.

-spec send_request(Name :: serverRef(), Request :: term()) -> requestId().
send_request(Name, Request) ->
   gen:send_request(Name, '$gen_call', Request).

-spec wait_response(RequestId :: requestId(), timeout()) ->
   {reply, Reply :: term()} | 'timeout' | {error, {Reason :: term(), serverRef()}}.
wait_response(RequestId, Timeout) ->
   gen:wait_response(RequestId, Timeout).

-spec check_response(Msg :: term(), RequestId :: requestId()) ->
   {reply, Reply :: term()} | 'no_reply' | {error, {Reason :: term(), serverRef()}}.
check_response(Msg, RequestId) ->
   gen:check_response(Msg, RequestId).

%% -----------------------------------------------------------------
%% Make a cast to a generic server.
%% -----------------------------------------------------------------
cast({global, Name}, Request) ->
   catch global:send(Name, cast_msg(Request)),
   ok;
cast({via, Module, Name}, Request) ->
   catch Module:send(Name, cast_msg(Request)),
   ok;
cast({Name, Node} = Dest, Request) when is_atom(Name), is_atom(Node) ->
   do_cast(Dest, Request);
cast(Dest, Request) when is_atom(Dest) ->
   do_cast(Dest, Request);
cast(Dest, Request) when is_pid(Dest) ->
   do_cast(Dest, Request).

do_cast(Dest, Request) ->
   do_send(Dest, cast_msg(Request)),
   ok.

cast_msg(Request) -> {'$gen_cast', Request}.

%% -----------------------------------------------------------------
%% Send a reply to the client.
%% -----------------------------------------------------------------
reply({To, Tag}, Reply) ->
   catch To ! {Tag, Reply},
   ok.

%% -----------------------------------------------------------------
%% Asynchronous broadcast, returns nothing, it's just send 'n' pray
%%-----------------------------------------------------------------
abcast(Name, Request) when is_atom(Name) ->
   do_abcast([node() | nodes()], Name, cast_msg(Request)).

abcast(Nodes, Name, Request) when is_list(Nodes), is_atom(Name) ->
   do_abcast(Nodes, Name, cast_msg(Request)).

do_abcast([Node | Nodes], Name, Msg) when is_atom(Node) ->
   do_send({Name, Node}, Msg),
   do_abcast(Nodes, Name, Msg);
do_abcast([], _, _) -> abcast.

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
multi_call(Name, Req)
   when is_atom(Name) ->
   do_multi_call([node() | nodes()], Name, Req, infinity).

multi_call(Nodes, Name, Req)
   when is_list(Nodes), is_atom(Name) ->
   do_multi_call(Nodes, Name, Req, infinity).

multi_call(Nodes, Name, Req, infinity) ->
   do_multi_call(Nodes, Name, Req, infinity);
multi_call(Nodes, Name, Req, Timeout)
   when is_list(Nodes), is_atom(Name), is_integer(Timeout), Timeout >= 0 ->
   do_multi_call(Nodes, Name, Req, Timeout).

%%% ---------------------------------------------------
%%% Send/receive functions
%%% ---------------------------------------------------
do_send(Dest, Msg) ->
   try erlang:send(Dest, Msg)
   catch
      error:_ -> ok
   end,
   ok.

do_multi_call(Nodes, Name, Req, infinity) ->
   Tag = make_ref(),
   Monitors = send_nodes(Nodes, Name, Tag, Req),
   rec_nodes(Tag, Monitors, Name, undefined);
do_multi_call(Nodes, Name, Req, Timeout) ->
   Tag = make_ref(),
   Caller = self(),
   Receiver =
      spawn(
         fun() ->
            %% Middleman process. Should be unsensitive to regular
            %% exit signals. The sychronization is needed in case
            %% the receiver would exit before the caller started
            %% the monitor.
            process_flag(trap_exit, true),
            Mref = erlang:monitor(process, Caller),
            receive
               {Caller, Tag} ->
                  Monitors = send_nodes(Nodes, Name, Tag, Req),
                  TimerId = erlang:start_timer(Timeout, self(), ok),
                  Result = rec_nodes(Tag, Monitors, Name, TimerId),
                  exit({self(), Tag, Result});
               {'DOWN', Mref, _, _, _} ->
                  %% Caller died before sending us the go-ahead.
                  %% Give up silently.
                  exit(normal)
            end
         end),
   Mref = erlang:monitor(process, Receiver),
   Receiver ! {self(), Tag},
   receive
      {'DOWN', Mref, _, _, {Receiver, Tag, Result}} ->
         Result;
      {'DOWN', Mref, _, _, Reason} ->
         %% The middleman code failed. Or someone did
         %% exit(_, kill) on the middleman process => Reason==killed
         exit(Reason)
   end.

send_nodes(Nodes, Name, Tag, Req) ->
   send_nodes(Nodes, Name, Tag, Req, []).

send_nodes([Node | Tail], Name, Tag, Req, Monitors)
   when is_atom(Node) ->
   Monitor = start_monitor(Node, Name),
   %% Handle non-existing names in rec_nodes.
   catch {Name, Node} ! {'$gen_call', {self(), {Tag, Node}}, Req},
   send_nodes(Tail, Name, Tag, Req, [Monitor | Monitors]);
send_nodes([_Node | Tail], Name, Tag, Req, Monitors) ->
   %% Skip non-atom Node
   send_nodes(Tail, Name, Tag, Req, Monitors);
send_nodes([], _Name, _Tag, _Req, Monitors) ->
   Monitors.

%% Against old nodes:
%% If no reply has been delivered within 2 secs. (per node) check that
%% the server really exists and wait for ever for the answer.
%%
%% Against contemporary nodes:
%% Wait for reply, server 'DOWN', or timeout from TimerId.

rec_nodes(Tag, Nodes, Name, TimerId) ->
   rec_nodes(Tag, Nodes, Name, [], [], 2000, TimerId).

rec_nodes(Tag, [{N, R} | Tail], Name, Badnodes, Replies, Time, TimerId) ->
   receive
      {'DOWN', R, _, _, _} ->
         rec_nodes(Tag, Tail, Name, [N | Badnodes], Replies, Time, TimerId);
      {{Tag, N}, Reply} ->  %% Tag is bound !!!
         erlang:demonitor(R, [flush]),
         rec_nodes(Tag, Tail, Name, Badnodes,
            [{N, Reply} | Replies], Time, TimerId);
      {timeout, TimerId, _} ->
         erlang:demonitor(R, [flush]),
         %% Collect all replies that already have arrived
         rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies)
   end;
rec_nodes(Tag, [N | Tail], Name, Badnodes, Replies, Time, TimerId) ->
   %% R6 node
   receive
      {nodedown, N} ->
         monitor_node(N, false),
         rec_nodes(Tag, Tail, Name, [N | Badnodes], Replies, 2000, TimerId);
      {{Tag, N}, Reply} ->  %% Tag is bound !!!
         receive {nodedown, N} -> ok after 0 -> ok end,
         monitor_node(N, false),
         rec_nodes(Tag, Tail, Name, Badnodes,
            [{N, Reply} | Replies], 2000, TimerId);
      {timeout, TimerId, _} ->
         receive {nodedown, N} -> ok after 0 -> ok end,
         monitor_node(N, false),
         %% Collect all replies that already have arrived
         rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies)
   after Time ->
      case rpc:call(N, erlang, whereis, [Name]) of
         Pid when is_pid(Pid) -> % It exists try again.
            rec_nodes(Tag, [N | Tail], Name, Badnodes,
               Replies, infinity, TimerId);
         _ -> % badnode
            receive {nodedown, N} -> ok after 0 -> ok end,
            monitor_node(N, false),
            rec_nodes(Tag, Tail, Name, [N | Badnodes],
               Replies, 2000, TimerId)
      end
   end;
rec_nodes(_, [], _, Badnodes, Replies, _, TimerId) ->
   case catch erlang:cancel_timer(TimerId) of
      false ->  % It has already sent it's message
         receive
            {timeout, TimerId, _} -> ok
         after 0 ->
            ok
         end;
      _ -> % Timer was cancelled, or TimerId was 'undefined'
         ok
   end,
   {Replies, Badnodes}.

%% Collect all replies that already have arrived
rec_nodes_rest(Tag, [{N, R} | Tail], Name, Badnodes, Replies) ->
   receive
      {'DOWN', R, _, _, _} ->
         rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies);
      {{Tag, N}, Reply} -> %% Tag is bound !!!
         erlang:demonitor(R, [flush]),
         rec_nodes_rest(Tag, Tail, Name, Badnodes, [{N, Reply} | Replies])
   after 0 ->
      erlang:demonitor(R, [flush]),
      rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies)
   end;
rec_nodes_rest(Tag, [N | Tail], Name, Badnodes, Replies) ->
   %% R6 node
   receive
      {nodedown, N} ->
         monitor_node(N, false),
         rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies);
      {{Tag, N}, Reply} ->  %% Tag is bound !!!
         receive {nodedown, N} -> ok after 0 -> ok end,
         monitor_node(N, false),
         rec_nodes_rest(Tag, Tail, Name, Badnodes, [{N, Reply} | Replies])
   after 0 ->
      receive {nodedown, N} -> ok after 0 -> ok end,
      monitor_node(N, false),
      rec_nodes_rest(Tag, Tail, Name, [N | Badnodes], Replies)
   end;
rec_nodes_rest(_Tag, [], _Name, Badnodes, Replies) ->
   {Replies, Badnodes}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API helpers  end  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% sys callbacks start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------
system_continue(Parent, Debug, [Name, Module, HibernateAfterTimeout, CurState, Timers, IsHib]) ->
   case IsHib of
      true ->
         proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, IsHib]);
      _ ->
         receiveIng(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, IsHib)
   end.

-spec system_terminate(_, _, _, [_]) -> no_return().
system_terminate(Reason, _Parent, Debug, [Name, Module, _HibernateAfterTimeout, CurState, Timers, _IsHib]) ->
   terminate(exit, Reason, ?STACKTRACE(), Name, Module, CurState, Debug, Timers, []).

system_code_change([Name, Module, HibernateAfterTimeout, CurState, Timers, IsHib], _Module, OldVsn, Extra) ->
   case catch Module:code_change(OldVsn, CurState, Extra) of
      {ok, NewState} -> {ok, [Name, Module, HibernateAfterTimeout, NewState, Timers, IsHib]};
      Else -> Else
   end.

system_get_state([_Name, _Module, _HibernateAfterTimeout, CurState, _Timers, _IsHib]) ->
   {ok, CurState}.

system_replace_state(StateFun, [Name, Module, HibernateAfterTimeout, CurState, Timers, IsHib]) ->
   NewState = StateFun(CurState),
   {ok, NewState, [Name, Module, HibernateAfterTimeout, NewState, Timers, IsHib]}.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% sys callbacks end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

start_monitor(Node, Name) when is_atom(Node), is_atom(Name) ->
   if node() =:= nonode@nohost, Node =/= nonode@nohost ->
      Ref = make_ref(),
      self() ! {'DOWN', Ref, process, {Name, Node}, noconnection},
      {Node, Ref};
      true ->
         case catch erlang:monitor(process, {Name, Node}) of
            {'EXIT', _} ->
               %% Remote node is R6
               monitor_node(Node, true),
               Node;
            Ref when is_reference(Ref) ->
               {Node, Ref}
         end
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% gen callbacks start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
doModuleInit(Module, Args) ->
   try
      Module:init(Args)
   catch
      throw:Ret -> Ret;
      Class:Reason:Stacktrace -> {'EXIT', Class, Reason, Stacktrace}
   end.

init_it(Starter, self, ServerRef, Module, Args, Options) ->
   init_it(Starter, self(), ServerRef, Module, Args, Options);
init_it(Starter, Parent, ServerRef, Module, Args, Options) ->
   Name = gen:name(ServerRef),
   Debug = gen:debug_options(Name, Options),
   HibernateAfterTimeout = gen:hibernate_after(Options),

   case doModuleInit(Module, Args) of
      {ok, State} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         receiveIng(Parent, Name, Module, HibernateAfterTimeout, State, Debug, #{}, false);
      {ok, State, Action} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         loopEntry(Parent, Name, Module, HibernateAfterTimeout, State, Debug, #{}, listify(Action));
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
         Error = {bad_return_value, _Ret},
         proc_lib:init_ack(Starter, {error, Error}),
         exit(Error)
   end.


%%-----------------------------------------------------------------
%% enter_loop(Module, Options, State, <ServerName>, <TimeOut>) ->_
%%
%% Description: Makes an existing process into a gen_server.
%%              The calling process will enter the gen_server receive
%%              loop and become a gen_server process.
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


-spec enter_loop(Module :: module(), State :: term(), Opts :: [enterLoopOpt()], Server :: serverName() | pid(), action() | [action()]) -> no_return().
enter_loop(Module, State, Opts, ServerName, Action) ->
   Name = gen:get_proc_name(ServerName),
   Parent = gen:get_parent(),
   Debug = gen:debug_options(Name, Opts),
   HibernateAfterTimeout = gen:hibernate_after(Opts),
   loopEntry(Parent, Name, Module, HibernateAfterTimeout, State, Debug, #{}, listify(Action)).

%%% Internal callbacks
wakeupFromHib(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, IsHib) ->
   %% 这是一条新消息，唤醒了我们，因此我们必须立即收到它
   receiveIng(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, IsHib).

loopEntry(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Actions) ->
   case doParseAL(Actions, Name, Debug, false, false, Timers) of
      {error, ErrorContent} ->
         terminate(error, ErrorContent, ?STACKTRACE(), Name, Module, CurState, Debug, Timers, []);
      {NewDebug, IsHib, DoAfter, NewTimers} ->
         case DoAfter of
            {doAfter, Args} ->
               doAfterCall(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, NewTimers, listHib(IsHib), Args);
            _ ->
               case IsHib of
                  true ->
                     proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, NewTimers, IsHib]);
                  _ ->
                     receiveIng(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, NewTimers, false)
               end
         end
   end,
   ok.

receiveIng(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, IsHib) ->
   receive
      Msg ->
         case Msg of
            {'$gen_call', From, Request} ->
               matchCallMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, From, Request);
            {'$gen_cast', Cast} ->
               matchCastMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Cast);
            {timeout, TimerRef, TimeoutName} ->
               case Timers of
                  #{TimeoutName := {TimerRef, TimeoutMsg}} ->
                     NewTimer = maps:remove(TimeoutName, Timers),
                     matchInfoMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, NewTimer, TimeoutMsg);
                  _ ->
                     matchInfoMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Msg)
               end;
            {system, PidFrom, Request} ->
               %% 不返回但尾递归调用 system_continue/3
               sys:handle_system_msg(Request, PidFrom, Parent, ?MODULE, Debug, [Name, Module, HibernateAfterTimeout, CurState, Timers, IsHib], IsHib);

            {'EXIT', Parent, Reason} ->
               terminate(Reason, Reason, ?STACKTRACE(), Name, Module, CurState, Debug, Timers, Msg);
            _ ->
               matchInfoMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Msg)
         end
   after HibernateAfterTimeout ->
      proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers])
   end.

matchCallMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, From, Request) ->
   MsgEvent = {{call, From}, Request},
   NewDebug = ?SYS_DEBUG(Debug, Name, {in, MsgEvent}),
   try Module:handleCall(Request, CurState, From) of
      Result ->
         handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, Result, From)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, Result, From);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, Name, Module, CurState, NewDebug, Timers, MsgEvent)
   end.

matchCastMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Cast) ->
   MsgEvent = {cast, Cast},
   NewDebug = ?SYS_DEBUG(Debug, Name, {in, MsgEvent}),
   try Module:handleCast(Cast, CurState) of
      Result ->
         handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, Result, false)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, Result, false);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, Name, Module, CurState, NewDebug, Timers, MsgEvent)
   end.

matchInfoMsg(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Msg) ->
   MsgEvent = {info, Msg},
   NewDebug = ?SYS_DEBUG(Debug, Name, {in, MsgEvent}),
   try Module:handleInfo(Msg, CurState) of
      Result ->
         handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, Result, false)
   catch
      throw:Result ->
         handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, Result, false);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, Name, Module, CurState, NewDebug, Timers, MsgEvent)
   end.

doAfterCall(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, LeftAction, Args) ->
   MsgEvent = {doAfter, Args},
   NewDebug = ?SYS_DEBUG(Debug, Name, {in, MsgEvent}),
   try Module:handleAfter(Args, CurState) of
      Result ->
         handleAR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, LeftAction, Result)
   catch
      throw:Result ->
         handleAR(Parent, Name, Module, HibernateAfterTimeout, CurState, NewDebug, Timers, LeftAction, Result);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, Name, Module, CurState, NewDebug, Timers, MsgEvent)
   end.

handleCR(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, Result, From) ->
   case Result of
      ok ->
         receiveIng(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, false);
      {noreply, NewState} ->
         receiveIng(Parent, Name, Module, HibernateAfterTimeout, NewState, Debug, Timers, false);
      {reply, Reply, NewState} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
         receiveIng(Parent, Name, Module, HibernateAfterTimeout, NewState, NewDebug, Timers, false);
      {noreply, NewState, Action} ->
         loopEntry(Parent, Name, Module, HibernateAfterTimeout, NewState, Debug, Timers, listify(Action));
      {stop, Reason, NewState} ->
         terminate(exit, Reason, ?STACKTRACE(), Name, Module, NewState, Debug, Timers, {return, stop});
      {reply, Reply, NewState, Action} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
         loopEntry(Parent, Name, Module, HibernateAfterTimeout, NewState, NewDebug, Timers, listify(Action));
      {stop, Reason, Reply, NewState} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, Name, {out, Reply, From, NewState}),
         terminate(exit, Reason, ?STACKTRACE(), Name, Module, NewState, NewDebug, Timers, {return, stop_reply})
   end.

handleAR(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, LeftAction, Result) ->
   case Result of
      ok ->
         loopEntry(Parent, Name, Module, HibernateAfterTimeout, CurState, Debug, Timers, LeftAction);
      {noreply, NewState} ->
         loopEntry(Parent, Name, Module, HibernateAfterTimeout, NewState, Debug, Timers, LeftAction);
      {noreply, NewState, Action} ->
         loopEntry(Parent, Name, Module, HibernateAfterTimeout, NewState, Debug, Timers, listify(Action) ++ LeftAction);
      {stop, Reason, NewState} ->
         terminate(exit, Reason, ?STACKTRACE(), Name, Module, NewState, Debug, Timers, {return, stop_reply})
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
      {'nTimeout', TimeoutName, Time, TimeoutMsg, Options} ->
         case Time of
            infinity ->
               NewTimers = doCancelTimer(TimeoutName, Timers),
               doParseAL(LeftActions, Name, Debug, IsHib, DoAfter, NewTimers);
            _ ->
               TimerRef = erlang:start_timer(Time, self(), TimeoutName, Options),
               NewDebug = ?SYS_DEBUG(Debug, Name, {start_timer, {TimeoutName, Time, TimeoutMsg, Options}}),
               NewTimers = doRegisterTimer(TimeoutName, TimerRef, TimeoutMsg, Timers),
               doParseAL(LeftActions, Name, NewDebug, IsHib, DoAfter, NewTimers)
         end;
      {'nTimeout', TimeoutName, Time, TimeoutMsg} ->
         case Time of
            infinity ->
               NewTimers = doCancelTimer(TimeoutName, Timers),
               doParseAL(LeftActions, Name, Debug, IsHib, DoAfter, NewTimers);
            _ ->
               TimerRef = erlang:start_timer(Time, self(), TimeoutName),
               NewDebug = ?SYS_DEBUG(Debug, Name, {start_timer, {TimeoutName, Time, TimeoutMsg, []}}),
               NewTimers = doRegisterTimer(TimeoutName, TimerRef, TimeoutMsg, Timers),
               doParseAL(LeftActions, Name, NewDebug, IsHib, DoAfter, NewTimers)
         end;
      {'uTimeout', TimeoutName, NewTimeoutMsg} ->
         NewTimers = doUpdateTimer(TimeoutName, NewTimeoutMsg, Timers),
         doParseAL(LeftActions, Name, Debug, IsHib, DoAfter, NewTimers);
      {'cTimeout', TimeoutName} ->
         NewTimers = doCancelTimer(TimeoutName, Timers),
         doParseAL(LeftActions, Name, Debug, IsHib, DoAfter, NewTimers);
      infinity ->
         ok;
      Timeout when is_integer(Timeout) ->
         erlang:send_after(Timeout, self(), timeout),
         NewDebug = ?SYS_DEBUG(Debug, Name, {start_timer, {timeout, Timeout, timeout, []}}),
         doParseAL(LeftActions, Name, NewDebug, IsHib, DoAfter, Timers);
      _ ->
         {error, {bad_ActionType, OneAction}}
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% timer deal start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

doRegisterTimer(TimeoutType, NewTimerRef, TimeoutMsg, Timers) ->
   case Timers of
      #{TimeoutType := {OldTimerRef, _OldTimeMsg}} ->
         TemTimers = cancelTimer(TimeoutType, OldTimerRef, Timers),
         TemTimers#{TimeoutType => {NewTimerRef, TimeoutMsg}};
      _ ->
         Timers#{TimeoutType => {NewTimerRef, TimeoutMsg}}
   end.

doCancelTimer(TimeoutType, Timers) ->
   case Timers of
      #{TimeoutType := {TimerRef, _TimeoutMsg}} ->
         cancelTimer(TimeoutType, TimerRef, Timers);
      _ ->
         Timers
   end.

doUpdateTimer(TimeoutType, Timers, TimeoutMsg) ->
   case Timers of
      #{TimeoutType := {TimerRef, _OldTimeoutMsg}} ->
         Timers#{TimeoutType := {TimerRef, TimeoutMsg}};
      _ ->
         Timers
   end.

cancelTimer(TimeoutType, TimerRef, Timers) ->
   case erlang:cancel_timer(TimerRef) of
      false ->                                     % 找不到计时器，我们还没有看到超时消息
         receive
            {timeout, TimerRef, TimeoutType} ->
               ok                                  % 丢弃该超时消息
         after 0 ->
            ok
         end;
      _ ->
         ok                                        % Timer 已经运行了
   end,
   maps:remove(TimeoutType, Timers).

listify(Item) when is_list(Item) ->
   Item;
listify(Item) ->
   [Item].

listHib(true) ->
   [hibernate];
listHib(_) ->
   [].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% timer deal end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


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
terminate(Class, Reason, Stacktrace, Name, Module, CurState, Debug, _Timers, MsgEvent) ->
   Reply = try_terminate(Module, terminate_reason(Class, Reason, Stacktrace), CurState),
   case Reply of
      {'EXIT', C, R, S} ->
         error_info({R, S}, Name, undefined, MsgEvent, Module, CurState, Debug),
         erlang:raise(C, R, S);
      _ ->
         case {Class, Reason} of
            {exit, normal} -> ok;
            {exit, shutdown} -> ok;
            {exit, {shutdown, _}} -> ok;
            _ ->
               error_info(Reason, Name, undefined, MsgEvent, Module, CurState, Debug)
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
            throw:R ->
               {ok, R};
            Class:R:Stacktrace ->
               {'EXIT', Class, R, Stacktrace}
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
error_info(Reason, Name, From, Msg, Module, State, Debug) ->
   Log = sys:get_log(Debug),
   ?LOG_ERROR(#{label=>{gen_server, terminate},
      name=>Name,
      last_message=>Msg,
      state=>format_status(terminate, Module, get(), State),
      log=>format_log_state(Module, Log),
      reason=>Reason,
      client_info=>client_stacktrace(From)},
      #{domain=>[otp],
         report_cb=>fun gen_server:format_log/2,
         error_logger=>#{tag=>error,
            report_cb=>fun gen_server:format_log/1}}),
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
limit_report(#{label:={gen_server, terminate},
   last_message:=Msg,
   state:=State,
   log:=Log,
   reason:=Reason,
   client_info:=Client} = Report,
   Depth) ->
   Report#{last_message=>io_lib:limit_term(Msg, Depth),
      state=>io_lib:limit_term(State, Depth),
      log=>[io_lib:limit_term(L, Depth) || L <- Log],
      reason=>io_lib:limit_term(Reason, Depth),
      client_info=>limit_client_report(Client, Depth)};
limit_report(#{label:={gen_server, no_handle_info},
   message:=Msg} = Report, Depth) ->
   Report#{message=>io_lib:limit_term(Msg, Depth)}.

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
         #{chars_limit:=unlimited} ->
            [];
         #{chars_limit:=Limit} ->
            [{chars_limit, Limit}]
      end,
   {Format, Args} = format_log_single(Report, FormatOpts),
   io_lib:format(Format, Args, IoOpts).

format_log_single(#{label:={gen_server, terminate},
   name:=Name,
   last_message:=Msg,
   state:=State,
   log:=Log,
   reason:=Reason,
   client_info:=Client},
   #{single_line:=true, depth:=Depth} = FormatOpts) ->
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
   {Format1 ++ ServerLogFormat ++ ClientLogFormat,
         Args1 ++ ServerLogArgs ++ ClientLogArgs};
format_log_single(#{label:={gen_server, no_handle_info},
   module:=Module,
   message:=Msg},
   #{single_line:=true, depth:=Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Format = lists:append(["Undefined handle_info in ", P,
      ". Unhandled message: ", P, "."]),
   Args =
      case Depth of
         unlimited ->
            [Module, Msg];
         _ ->
            [Module, Depth, Msg, Depth]
      end,
   {Format, Args};
format_log_single(Report, FormatOpts) ->
   format_log_multi(Report, FormatOpts).

format_log_multi(#{label:={gen_server, terminate},
   name:=Name,
   last_message:=Msg,
   state:=State,
   log:=Log,
   reason:=Reason,
   client_info:=Client},
   #{depth:=Depth} = FormatOpts) ->
   Reason1 = fix_reason(Reason),
   {ClientFmt, ClientArgs} = format_client_log(Client, FormatOpts),
   P = p(FormatOpts),
   Format =
      lists:append(
         ["** Generic server ", P, " terminating \n"
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
            [Name, Msg, State, Reason1] ++
               case Log of
                  [] -> [];
                  _ -> Log
               end ++ ClientArgs;
         _ ->
            [Name, Depth, Msg, Depth, State, Depth, Reason1, Depth] ++
               case Log of
                  [] -> [];
                  _ -> lists:flatmap(fun(L) -> [L, Depth] end, Log)
               end ++ ClientArgs
      end,
   {Format, Args};
format_log_multi(#{label:={gen_server, no_handle_info},
   module:=Module,
   message:=Msg},
   #{depth:=Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Format =
      "** Undefined handle_info in ~p~n"
      "** Unhandled message: " ++ P ++ "~n",
   Args =
      case Depth of
         unlimited ->
            [Module, Msg];
         _ ->
            [Module, Msg, Depth]
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

p(#{single_line:=Single, depth:=Depth, encoding:=Enc}) ->
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
   [PDict, SysState, Parent, Debug, [Name, State, Module, _Time, _HibernateAfterTimeout]] = StatusData,
   Header = gen:format_status_header("Status for generic server", Name),
   Log = sys:get_log(Debug),
   Specific =
      case format_status(Opt, Module, PDict, State) of
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
         case catch Module:format_status(Opt, [PDict, State]) of
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