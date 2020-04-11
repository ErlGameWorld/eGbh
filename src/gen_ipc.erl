-module(gen_ipc).

-include_lib("kernel/include/logger.hrl").

-export([
   %% API for gen_server or gen_statem behaviour
   start/3, start/4, start_link/3, start_link/4
   , stop/1, stop/3
   , cast/2
   , abcast/2, abcast/3
   , call/2, call/3
   , multi_call/2, multi_call/3, multi_call/4
   , enter_loop/4, enter_loop/5, enter_loop/6
   , reply/1, reply/2

   %% API for gen_event behaviour
   , infoNotify/2, callNotify/2
   , epmCall/3, epmCall/4, epmInfo/3
   , addEpm/3, addSupEpm/3, whichEpm/1
   , deleteEpm/3, swapEpm/3, swapSupEpm/3

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
   , wakeup_from_hibernate/5
   %% logger callback
   , format_log/1
   , epm_log/1
]).

%% 进程字典的宏定义
-define(PD_PARENT, '$pd_parent').
-define(PD_PRO_NAME, '$pd_proname').
-define(PD_EPM_FLAG, '$pd_epmFlag').
-define(PD_EPM_LIST, '$pd_epmList').

%% timeout相关宏定义
-define(REL_TIMEOUT(T), ((is_integer(T) andalso (T) >= 0) orelse (T) =:= infinity)).
-define(ABS_TIMEOUT(T), (is_integer(T) orelse (T) =:= infinity)).

-define(STACKTRACE(), element(2, erlang:process_info(self(), current_stacktrace))).

-define(CB_FORM_ENTER, 1).             %% 从enter 回调返回
-define(CB_FORM_AFTER, 2).             %% 从after 回调返回
-define(CB_FORM_EVENT, 3).             %% 从event 回调返回

%% debug 调试相关宏定义
-define(NOT_DEBUG, []).
-define(SYS_DEBUG(Debug, SystemEvent),
   case Debug of
      ?NOT_DEBUG ->
         Debug;
      _ ->
         sys:handle_debug(Debug, fun print_event/3, getProName(), SystemEvent)
   end).

%%%==========================================================================
%%% Interface functions.
%%%==========================================================================
%% gen:call 发送消息来源进程格式类型
-type from() :: {To :: pid(), Tag :: term()}.

%% 事件类型
-type eventType() :: externalEventType() | timeoutEventType() | {'onevent', Subtype :: term()}.
-type externalEventType() :: {'call', From :: from()} | 'cast' | 'info'.
-type timeoutEventType() :: 'eTimeout' | 'sTimeout' | {'gTimeout', Name :: term()}.

%% 是否改进程捕捉信号 gen_event管理进程需要设置该参数为true
-type isTrapExit() :: boolean().
%% 是否允许进入enter 回调
-type isEnter() :: boolean().
%% 如果为 "true" 则推迟当前事件，并在状态更改时重试(=/=)
-type isPostpone() :: boolean().
%% 如果为 "true" 则使服务器休眠而不是进入接收状态
-type isHibernate() :: boolean().

%% 定时器相关
-type timeouts() :: Time :: timeout() | integer().
-type timeoutOption() :: {abs, Abs :: boolean()}.

% gen_event模式下回调模块的Key
-type epmHandler() :: atom() | {atom(), term()}.

%% 在状态更改期间:
%% NextStatus and NewData are set.
%% 按照出现的顺序处理 actions()列表
%% 这些action() 按包含列表中的出现顺序执行。设置选项的选项将覆盖任何以前的选项，因此每种类型的最后一种将获胜。
%% 如果设置了enter 则进入enter回调
%% 如果设置了doAfter 则进入after回调
%% 如果 "postpone" 为 "true"，则推迟当前事件。
%% 如果设置了“超时”，则开始状态超时。 零超时事件将会插入到待处理事件的前面 先执行
%% 如果有postponed 事件则 事件执行顺序为 超时添加和更新 + 零超时 + 当前事件 + 反序的Postpone事件 + LeftEvent
%% 处理待处理的事件，或者如果没有待处理的事件，则服务器进入接收或休眠状态（当“hibernate”为“ true”时）
-type initAction() ::
   {trap_exit, Bool :: isTrapExit()} |                                      % 设置是否捕捉信息 主要用于gen_event模式下
   eventAction().

-type eventAction() ::
   {'doAfter', Args :: term()} |                                           % 设置执行某事件后是否回调 handleAfter
   {'isPostpone', Bool :: isPostpone()} |                                  % 设置推迟选项
   {'nextEvent', EventType :: eventType(), EventContent :: term()} |       % 插入事件作为下一个处理
   commonAction().

-type afterAction() ::
   {'nextEvent', EventType :: eventType(), EventContent :: term()} |       % 插入事件作为下一个处理
   commonAction().

-type enterAction() ::
   {'isPostpone', false} |                                                  % 虽然enter action 不能设置postpone 但是可以取消之前event的设置
   commonAction().

-type commonAction() ::
   {'isEnter', Bool :: isEnter()} |
   {'isHibernate', Bool :: isHibernate()} |
   timeoutAction() |
   replyAction().

-type timeoutAction() ::
   timeoutNewAction() |
   timeoutCancelAction() |
   timeoutUpdateAction().

-type timeoutNewAction() ::
   {'eTimeout', Time :: timeouts(), EventContent :: term()} |                                                          % Set the event_timeout option
   {'eTimeout', Time :: timeouts(), EventContent :: term(), Options :: ([timeoutOption()])} |                          % Set the generic_timeout option
   {{'gTimeout', Name :: term()}, Time :: timeouts(), EventContent :: term()} |                                        % Set the generic_timeout option
   {{'gTimeout', Name :: term()}, Time :: timeouts(), EventContent :: term(), Options :: ([timeoutOption()])} |        % Set the status_timeout option
   {'sTimeout', Time :: timeouts(), EventContent :: term()} |                                                          % Set the status_timeout option
   {'sTimeout', Time :: timeouts(), EventContent :: term(), Options :: ([timeoutOption()])}.

-type timeoutCancelAction() ::
   'c_eTimeout' |
   {'c_gTimeout', Name :: term()} |
   'c_sTimeout'.

-type timeoutUpdateAction() ::
   {'u_eTimeout', EventContent :: term()} |
   {{'u_gTimeout', Name :: term()}, EventContent :: term()} |
   {'u_sTimeout', EventContent :: term()}.

-type replyAction() ::
   {'reply', From :: from(), Reply :: term()}.

-type eventCallbackResult() ::
   {'reply', Reply :: term(), NewState :: term()} |                                                                    % 用作gen_server模式时快速响应进入消息接收
   {'sreply', Reply :: term(), NextStatus :: term(), NewState :: term()} |                                             % 用作gen_ipc模式便捷式返回reply 而不用把reply放在actions列表中
   {'noreply', NewState :: term()} |                                                                                   % 用作gen_server模式时快速响应进入消息接收
   {'reply', Reply :: term(), NewState :: term(), Options :: hibernate | {doAfter, Args}} |                            % 用作gen_server模式时快速响应进入消息接收
   {'sreply', Reply :: term(), NextStatus :: term(), NewState :: term(), Actions :: [eventAction(), ...]} |            % 用作gen_ipc模式便捷式返回reply 而不用把reply放在actions列表中
   {'noreply', NewState :: term(), Options :: hibernate | {doAfter, Args}} |                                           % 用作gen_server模式时快速响应进入循环
   {'nextStatus', NextStatus :: term(), NewState :: term()} |                                                          % {next_status,NextStatus,NewData,[]}
   {'nextStatus', NextStatus :: term(), NewState :: term(), Actions :: [eventAction(), ...]} |                         % Status transition, maybe to the same status
   commonCallbackResult(eventAction()).

-type afterCallbackResult() ::
   {'nextStatus', NextStatus :: term(), NewState :: term()} |                                                          % {next_status,NextStatus,NewData,[]}
   {'nextStatus', NextStatus :: term(), NewState :: term(), Actions :: [afterAction(), ...]} |                         % Status transition, maybe to the same status
   {'noreply', NewState :: term()} |                                                                                   % 用作gen_server模式时快速响应进入消息接收
   {'noreply', NewState :: term(), Options :: hibernate} |                                                             % 用作gen_server模式时快速响应进入消息接收
   commonCallbackResult(afterAction()).

-type enterCallbackResult() ::
   commonCallbackResult(enterAction()).

-type commonCallbackResult(ActionType) ::
   {'keepStatus', NewState :: term()} |                                                                                % {keep_status,NewData,[]}
   {'keepStatus', NewState :: term(), Actions :: [ActionType]} |                                                       % Keep status, change data
   'keepStatusState' |                                                                                                 % {keep_status_and_data,[]}
   {'keepStatusState', Actions :: [ActionType]} |                                                                      % Keep status and data -> only actions
   {'repeatStatus', NewState :: term()} |                                                                              % {repeat_status,NewData,[]}
   {'repeatStatus', NewState :: term(), Actions :: [ActionType]} |                                                     % Repeat status, change data
   'repeatStatusState' |                                                                                               % {repeat_status_and_data,[]}
   {'repeatStatusState', Actions :: [ActionType]} |                                                                    % Repeat status and data -> only actions
   'stop' |                                                                                                            % {stop,normal}
   {'stop', Reason :: term()} |                                                                                        % Stop the server
   {'stop', Reason :: term(), NewState :: term()} |                                                                    % Stop the server
   {'stopReply', Reason :: term(), Replies :: replyAction() | [replyAction(), ...]} |                                                  % Reply then stop the server
   {'stopReply', Reason :: term(), Replies :: replyAction() | [replyAction(), ...], NewState :: term()}.                               % Reply then stop the server

%% 状态机的初始化功能函数
%% 如果要模拟gen_server init返回定时时间 可以在Actions返回定时动作
%% 如果要把改进程当做gen_event管理进程需要在actions列表包含 {trap_exit, true} 设置该进程捕捉异常
-callback init(Args :: term()) ->
   'ignore' |
   {'stop', Reason :: term()} |
   {'ok', State :: term()} |
   {'ok', Status :: term(), State :: term()} |
   {'ok', Status :: term(), State :: term(), Actions :: [initAction(), ...]}.

%% 当 enter call 回调函数
-callback handleEnter(OldStatus :: term(), CurStatus :: term(), State :: term()) ->
   enterCallbackResult().

%% 当 init返回actions包含 doAfter 的时候会在 enter调用后 调用该函数 或者
%% 在事件回调函数返回后 enter调用后调用该函数
%% 该回调函数相当于 gen_server 的 handle_continue回调 但是在综合模式时 也可以生效
-callback handleAfter(AfterArgs :: term(), Status :: term(), State :: term()) ->
   afterCallbackResult().

%% call 所以状态的回调函数
-callback handleCall(EventContent :: term(), Status :: term(), State :: term(), From :: {pid(), Tag :: term()}) ->
   eventCallbackResult().

%% cast 回调函数
-callback handleCast(EventContent :: term(), Status :: term(), State :: term()) ->
   eventCallbackResult().

%% info 回调函数
-callback handleInfo(EventContent :: term(), Status :: term(), State :: term()) ->
   eventCallbackResult().

%% 内部事件 Onevent 包括actions 设置的定时器超时产生的事件 和 nextEvent产生的超时事件 但是不是 call cast info 回调函数 以及其他自定义定时事件 的回调函数
%% 并且这里需要注意 其他erlang:start_timer生成超时事件发送的消息 不能和gen_ipc定时器关键字重合 有可能会导致一些问题
-callback handleOnevent(EventType :: term(), EventContent :: term(), Status :: term(), State :: term()) ->
   eventCallbackResult().

%% 在gen_event模式下 扩展了下面三个回调函数 考虑场景是：
%% 比如游戏里的公会 有时候一个公会一个进程来管理可能开销比较高 只用一个进程来管理所以公会有可能一个进程处理不过来
%% 这个时候可以考虑用gen_ipc来分组管理 一个gen_ipc进程管理 N 个公会 但是管理进程需要做一些数据保存什么 或者定时 就可能会用到下面的这些函数
%% gen_event模式时 notify 有可能需要回调该管理进程的该函数
-callback handleEpmEvent(EventContent :: term(), Status :: term(), State :: term()) ->
   eventCallbackResult().

%% gen_event模式时 call请求 有可能需要回调该管理进程的该函数
-callback handleEpmCall(EventContent :: term(), Status :: term(), State :: term()) ->
   eventCallbackResult().

%% gen_event模式时 info消息 有可能需要回调该管理进程的该函数
-callback handleEpmInfo(EventContent :: term(), Status :: term(), State :: term()) ->
   eventCallbackResult().

%% 在服务器终止之前进行清理。
-callback terminate(Reason :: 'normal' | 'shutdown' | {'shutdown', term()} | term(), Status :: term(), State :: term()) ->
   any().

%% 代码更新回调函数
-callback code_change(OldVsn :: term() | {'down', term()}, OldStatus :: term(), OldState :: term(), Extra :: term()) ->
   {ok, NewStatus :: term(), NewData :: term()} |
   (Reason :: term()).

%% 以一种通常被精简的方式来格式化回调模块状态。
%% 对于StatusOption =:= 'normal'，首选返回 term 是[{data,[{"Status",FormattedStatus}]}]
%% 对于StatusOption =:= 'terminate'，它只是FormattedStatus
-callback formatStatus(StatusOption, [PDict | term()]) ->
   Status :: term() when
   StatusOption :: 'normal' | 'terminate',
   PDict :: [{Key :: term(), Value :: term()}].

-optional_callbacks([
   formatStatus/2
   , terminate/3
   , code_change/4
   , handleEnter/3
   , handleAfter/3
   , handleOnevent/4
   , handleEpmEvent/3
   , handleEpmCall/3
   , handleEpmInfo/3
]).

-record(cycleData, {
   module :: atom()
   , isEnter = false :: boolean()
   , hibernateAfter = infinity :: timeout()
   , postponed = [] :: [{eventType(), term()}]
   , timers = #{} :: #{TimeoutType :: timeoutEventType() => {TimerRef :: reference(), TimeoutMsg :: term()}}
}).

-record(epmHer, {
   epmM :: atom(),
   epmId = false,
   epmS :: term(),
   epmSup = false :: 'false' | pid()}).

-compile({inline, [isEventType/1]}).

isEventType(Type) ->
   case Type of
      {'call', {_Pid, _Ref}} -> true;
      'cast' -> true;
      'info' -> true;
      {'onevent', _SubType} -> true;
      'eTimeout' -> true;
      'sTimeout' -> true;
      {'gTimeout', _Name} -> true;
      _ ->
         false
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% start stop API start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-type serverName() ::
   {'local', atom()} |
   {'global', GlobalName :: term()} |
   {'via', RegMod :: module(), Name :: term()}.

-type serverRef() ::
   pid()   |
   (LocalName :: atom()) |
   {Name :: atom(), Node :: atom()} |
   {'global', GlobalName :: term()} |
   {'via', RegMod :: module(), ViaName :: term()}.

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
   {'error', term()}.

-spec start(Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start(Module, Args, Opts) ->
   gen:start(?MODULE, nolink, Module, Args, Opts).

-spec start(ServerName :: serverName(), Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start(ServerName, Module, Args, Opts) ->
   gen:start(?MODULE, nolink, ServerName, Module, Args, Opts).

-spec start_link(Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start_link(Module, Args, Opts) ->
   gen:start(?MODULE, link, Module, Args, Opts).

-spec start_link(ServerName :: serverName(), Module :: module(), Args :: term(), Opts :: [startOpt()]) -> startRet().
start_link(ServerName, Module, Args, Opts) ->
   gen:start(?MODULE, link, ServerName, Module, Args, Opts).


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
      Class:Reason:Stacktrace -> {'EXIT', Class, Reason, Stacktrace}
   end.

init_it(Starter, self, ServerRef, Module, Args, Opts) ->
   init_it(Starter, self(), ServerRef, Module, Args, Opts);
init_it(Starter, Parent, ServerRef, Module, Args, Opts) ->
   Name = gen:name(ServerRef),
   Debug = gen:debug_options(Name, Opts),
   HibernateAfterTimeout = gen:hibernate_after(Opts),
   case doModuleInit(Module, Args) of
      {ok, State} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         loopEntry(Parent, Debug, Module, Name, HibernateAfterTimeout, undefined, State, []);
      {ok, Status, State} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         loopEntry(Parent, Debug, Module, Name, HibernateAfterTimeout, Status, State, []);
      {ok, Status, State, Actions} ->
         proc_lib:init_ack(Starter, {ok, self()}),
         loopEntry(Parent, Debug, Module, Name, HibernateAfterTimeout, Status, State, Actions);
      {stop, Reason} ->
         gen:unregister_name(ServerRef),
         proc_lib:init_ack(Starter, {error, Reason}),
         exit(Reason);
      ignore ->
         gen:unregister_name(ServerRef),
         proc_lib:init_ack(Starter, ignore),
         exit(normal);
      {'EXIT', Class, Reason, Stacktrace} ->
         gen:unregister_name(ServerRef),
         proc_lib:init_ack(Starter, {error, Reason}),
         error_info(Class, Reason, Stacktrace, #cycleData{module = Module}, 'Sun_init', '$un_init', Debug, []),
         erlang:raise(Class, Reason, Stacktrace);
      _Ret ->
         gen:unregister_name(ServerRef),
         Error = {badReturnFrom_doModuleInit, _Ret},
         proc_lib:init_ack(Starter, {error, Error}),
         error_info(error, Error, ?STACKTRACE(), #cycleData{module = Module}, '$un_init', '$un_init', Debug, []),
         exit(Error)
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% gen callbacks end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 进入循环 调用该进程必须使用proc_lib启动 且已经初始化状态和数据 包括注册名称
-spec enter_loop(Module :: module(), Status :: term(), State :: term(), Opts :: [enterLoopOpt()]) -> no_return().
enter_loop(Module, Status, State, Opts) ->
   enter_loop(Module, Status, State, Opts, self(), []).

-spec enter_loop(Module :: module(), Status :: term(), State :: term(), Opts :: [enterLoopOpt()], ServerOrActions :: serverName() | pid() | [eventAction()]) -> no_return().
enter_loop(Module, Status, State, Opts, ServerOrActions) ->
   if
      is_list(ServerOrActions) ->
         enter_loop(Module, Opts, Status, State, self(), ServerOrActions);
      true ->
         enter_loop(Module, Opts, Status, State, ServerOrActions, [])
   end.

-spec enter_loop(Module :: module(), Status :: term(), State :: term(), Opts :: [enterLoopOpt()], Server :: serverName() | pid(), Actions :: [eventAction()]) -> no_return().
enter_loop(Module, Status, State, Opts, ServerName, Actions) ->
   is_atom(Module) orelse error({atom, Module}),
   Parent = gen:get_parent(),
   Name = gen:get_proc_name(ServerName),
   Debug = gen:debug_options(Name, Opts),
   HibernateAfterTimeout = gen:hibernate_after(Opts),
   loopEntry(Parent, Debug, Module, Name, HibernateAfterTimeout, Status, State, Actions).

%% 这里的 init_it/6 和 enter_loop/5,6,7 函数汇聚
loopEntry(Parent, Debug, Module, Name, HibernateAfterTimeout, CurStatus, CurState, Actions) ->
   setParent(Parent),
   setProName(Name),
   %% 如果该进程用于 gen_event 则需要设置  process_flag(trap_exit, true) 需要在Actions返回 {trap_exit, true}
   MewActions =
      case lists:keyfind(trap_exit, 1, Actions) of
         false ->
            Actions;
         {trap_exit, true} ->
            process_flag(trap_exit, true),
            lists:keydelete(trap_exit, 1, Actions);
         _ ->
            lists:keydelete(trap_exit, 1, Actions)
      end,

   CycleData = #cycleData{module = Module, hibernateAfter = HibernateAfterTimeout},
   NewDebug = ?SYS_DEBUG(Debug, {enter, CurStatus}),
   %% 强制执行{postpone，false}以确保我们的假事件被丢弃
   LastActions = MewActions ++ [{isPostpone, false}],
   parseEventActionsList(CycleData, CurStatus, CurState, CurStatus, NewDebug, [{onevent, init_status}], true, LastActions, ?CB_FORM_EVENT).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% sys callbacks start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
system_continue(Parent, Debug, {CycleData, CurStatus, CurState, IsHibernate}) ->
   updateParent(Parent),
   if
      IsHibernate ->
         proc_lib:hibernate(?MODULE, wakeup_from_hibernate, [CycleData, CurStatus, CurState, Debug, IsHibernate]);
      true ->
         receiveMsgWait(CycleData, CurStatus, CurState, Debug, IsHibernate)
   end.

system_terminate(Reason, Parent, Debug, {CycleData, CurStatus, CurState, _IsHibernate}) ->
   updateParent(Parent),
   terminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, []).

system_code_change({#cycleData{module = Module} = CycleData, CurStatus, CurState, IsHibernate}, _Mod, OldVsn, Extra) ->
   case
      try Module:code_change(OldVsn, CurStatus, CurState, Extra)
      catch
         throw:Result -> Result
      end
   of
      {ok, NewStatus, NewState} ->
         {ok, {CycleData, NewStatus, NewState, IsHibernate}};
      Error ->
         Error
   end.

system_get_state({_CycleData, CurStatus, CurState, _IsHibernate}) ->
   {ok, {CurStatus, CurState}}.

system_replace_state(StatusFun, {CycleData, CurStatus, CurState, IsHibernate}) ->
   {NewStatus, NewState} = StatusFun(CurStatus, CurState),
   {ok, {NewStatus, NewState}, {CycleData, NewStatus, NewState, IsHibernate}}.

format_status(Opt, [PDict, SysStatus, Parent, Debug, {#cycleData{timers = Timers, postponed = Postponed} = CycleData, CurStatus, CurState, _IsHibernate}]) ->
   Header = gen:format_status_header("Status for status machine", getProName()),
   updateParent(Parent),
   Log = sys:get_log(Debug),
   [
      {header, Header},
      {data,
         [
            {"Status", SysStatus},
            {"Parent", Parent},
            {"Time-outs", list_timeouts(Timers)},
            {"Logged Events", Log},
            {"Postponed", Postponed}
         ]
      } |
      case format_status(Opt, PDict, CycleData, CurStatus, CurState) of
         L when is_list(L) -> L;
         T -> [T]
      end
   ].
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% sys callbacks end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API helpers  start  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
-spec call(ServerRef :: serverRef(), Request :: term()) -> Reply :: term().
call(ServerRef, Request) ->
   try gen:call(ServerRef, '$gen_call', Request) of
      {ok, Reply} ->
         Reply
   catch
      Class:Reason:Stacktrace ->
         erlang:raise(Class, {Reason, {?MODULE, call, [ServerRef, Request]}}, Stacktrace)
   end.

-spec call(ServerRef :: serverRef(), Request :: term(), Timeout :: timeout() |{'cleanTimeout', T :: timeout()} | {'dirtyTimeout', T :: timeout()}) -> Reply :: term().
call(ServerRef, Request, infinity) ->
   try gen:call(ServerRef, '$gen_call', Request, infinity) of
      {ok, Reply} ->
         Reply
   catch
      Class:Reason:Stacktrace ->
         erlang:raise(Class, {Reason, {?MODULE, call, [ServerRef, Request, infinity]}}, Stacktrace)
   end;
call(ServerRef, Request, {dirtyTimeout, T} = Timeout) ->
   try gen:call(ServerRef, '$gen_call', Request, T) of
      {ok, Reply} ->
         Reply
   catch
      Class:Reason:Stacktrace ->
         erlang:raise(Class, {Reason, {?MODULE, call, [ServerRef, Request, Timeout]}}, Stacktrace)
   end;
call(ServerRef, Request, {cleanTimeout, T} = Timeout) ->
   callClean(ServerRef, Request, Timeout, T);
call(ServerRef, Request, {_, _} = Timeout) ->
   erlang:error(badarg, [ServerRef, Request, Timeout]);
call(ServerRef, Request, Timeout) ->
   callClean(ServerRef, Request, Timeout, Timeout).

callClean(ServerRef, Request, Timeout, T) ->
   %% 通过代理过程呼叫服务器以躲避任何较晚的答复
   Ref = make_ref(),
   Self = self(),
   Pid = spawn(
      fun() ->
         Self !
            try gen:call(ServerRef, '$gen_call', Request, T) of
               Result ->
                  {Ref, Result}
            catch Class:Reason:Stacktrace ->
               {Ref, Class, Reason, Stacktrace}
            end
      end),
   Mref = monitor(process, Pid),
   receive
      {Ref, Result} ->
         demonitor(Mref, [flush]),
         case Result of
            {ok, Reply} ->
               Reply
         end;
      {Ref, Class, Reason, Stacktrace} ->
         demonitor(Mref, [flush]),
         erlang:raise(Class, {Reason, {?MODULE, call, [ServerRef, Request, Timeout]}}, Stacktrace);
      {'DOWN', Mref, _, _, Reason} ->
         %% 从理论上讲，有可能在try-of和！之间杀死代理进程。因此，在这种情况下
         exit(Reason)
   end.

multi_call(Name, Request) when is_atom(Name) ->
   do_multi_call([node() | nodes()], Name, Request, infinity).

multi_call(Nodes, Name, Request) when is_list(Nodes), is_atom(Name) ->
   do_multi_call(Nodes, Name, Request, infinity).

multi_call(Nodes, Name, Request, infinity) ->
   do_multi_call(Nodes, Name, Request, infinity);
multi_call(Nodes, Name, Request, Timeout) when is_list(Nodes), is_atom(Name), is_integer(Timeout), Timeout >= 0 ->
   do_multi_call(Nodes, Name, Request, Timeout).

do_multi_call(Nodes, Name, Request, infinity) ->
   Tag = make_ref(),
   Monitors = send_nodes(Nodes, Name, Tag, Request, []),
   rec_nodes(Tag, Monitors, Name, undefined);
do_multi_call(Nodes, Name, Request, Timeout) ->
   Tag = make_ref(),
   Caller = self(),
   Receiver = spawn(
      fun() ->
         process_flag(trap_exit, true),
         Mref = erlang:monitor(process, Caller),
         receive
            {Caller, Tag} ->
               Monitors = send_nodes(Nodes, Name, Tag, Request, []),
               TimerId = erlang:start_timer(Timeout, self(), ok),
               Result = rec_nodes(Tag, Monitors, Name, TimerId),
               exit({self(), Tag, Result});
            {'DOWN', Mref, _, _, _} ->
               exit(normal)
         end
      end
   ),
   Mref = erlang:monitor(process, Receiver),
   Receiver ! {self(), Tag},
   receive
      {'DOWN', Mref, _, _, {Receiver, Tag, Result}} ->
         Result;
      {'DOWN', Mref, _, _, Reason} ->
         exit(Reason)
   end.

send_nodes([Node | Tail], Name, Tag, Request, Monitors) when is_atom(Node) ->
   Monitor = start_monitor(Node, Name),
   catch {Name, Node} ! {'$gen_call', {self(), {Tag, Node}}, Request},
   send_nodes(Tail, Name, Tag, Request, [Monitor | Monitors]);
send_nodes([_Node | Tail], Name, Tag, Request, Monitors) ->
   send_nodes(Tail, Name, Tag, Request, Monitors);
send_nodes([], _Name, _Tag, _Req, Monitors) ->
   Monitors.

rec_nodes(Tag, Nodes, Name, TimerId) ->
   rec_nodes(Tag, Nodes, Name, [], [], 2000, TimerId).

rec_nodes(Tag, [{N, R} | Tail], Name, BadNodes, Replies, Time, TimerId) ->
   receive
      {'DOWN', R, _, _, _} ->
         rec_nodes(Tag, Tail, Name, [N | BadNodes], Replies, Time, TimerId);
      {{Tag, N}, Reply} ->
         erlang:demonitor(R, [flush]),
         rec_nodes(Tag, Tail, Name, BadNodes,
            [{N, Reply} | Replies], Time, TimerId);
      {timeout, TimerId, _} ->
         erlang:demonitor(R, [flush]),
         rec_nodes_rest(Tag, Tail, Name, [N | BadNodes], Replies)
   end;
rec_nodes(Tag, [N | Tail], Name, BadNodes, Replies, Time, TimerId) ->
   receive
      {nodedown, N} ->
         monitor_node(N, false),
         rec_nodes(Tag, Tail, Name, [N | BadNodes], Replies, 2000, TimerId);
      {{Tag, N}, Reply} ->
         receive {nodedown, N} -> ok after 0 -> ok end,
         monitor_node(N, false),
         rec_nodes(Tag, Tail, Name, BadNodes,
            [{N, Reply} | Replies], 2000, TimerId);
      {timeout, TimerId, _} ->
         receive {nodedown, N} -> ok after 0 -> ok end,
         monitor_node(N, false),
         rec_nodes_rest(Tag, Tail, Name, [N | BadNodes], Replies)
   after Time ->
      case rpc:call(N, erlang, whereis, [Name]) of
         Pid when is_pid(Pid) ->
            rec_nodes(Tag, [N | Tail], Name, BadNodes,
               Replies, infinity, TimerId);
         _ ->
            receive {nodedown, N} -> ok after 0 -> ok end,
            monitor_node(N, false),
            rec_nodes(Tag, Tail, Name, [N | BadNodes],
               Replies, 2000, TimerId)
      end
   end;
rec_nodes(_, [], _, BadNodes, Replies, _, TimerId) ->
   case catch erlang:cancel_timer(TimerId) of
      false ->
         receive
            {timeout, TimerId, _} -> ok
         after 0 ->
            ok
         end;
      _ ->
         ok
   end,
   {Replies, BadNodes}.

rec_nodes_rest(Tag, [{N, R} | Tail], Name, BadNodes, Replies) ->
   receive
      {'DOWN', R, _, _, _} ->
         rec_nodes_rest(Tag, Tail, Name, [N | BadNodes], Replies);
      {{Tag, N}, Reply} ->
         erlang:demonitor(R, [flush]),
         rec_nodes_rest(Tag, Tail, Name, BadNodes, [{N, Reply} | Replies])
   after 0 ->
      erlang:demonitor(R, [flush]),
      rec_nodes_rest(Tag, Tail, Name, [N | BadNodes], Replies)
   end;
rec_nodes_rest(Tag, [N | Tail], Name, BadNodes, Replies) ->
   receive
      {nodedown, N} ->
         monitor_node(N, false),
         rec_nodes_rest(Tag, Tail, Name, [N | BadNodes], Replies);
      {{Tag, N}, Reply} ->
         receive {nodedown, N} -> ok after 0 -> ok end,
         monitor_node(N, false),
         rec_nodes_rest(Tag, Tail, Name, BadNodes, [{N, Reply} | Replies])
   after 0 ->
      receive {nodedown, N} -> ok after 0 -> ok end,
      monitor_node(N, false),
      rec_nodes_rest(Tag, Tail, Name, [N | BadNodes], Replies)
   end;
rec_nodes_rest(_Tag, [], _Name, BadNodes, Replies) ->
   {Replies, BadNodes}.

start_monitor(Node, Name) when is_atom(Node), is_atom(Name) ->
   if node() =:= nonode@nohost, Node =/= nonode@nohost ->
      Ref = make_ref(),
      self() ! {'DOWN', Ref, process, {Name, Node}, noconnection},
      {Node, Ref};
      true ->
         case catch erlang:monitor(process, {Name, Node}) of
            {'EXIT', _} ->
               monitor_node(Node, true),
               Node;
            Ref when is_reference(Ref) ->
               {Node, Ref}
         end
   end.

-spec cast(ServerRef :: serverRef(), Msg :: term()) -> ok.
cast({global, Name}, Msg) ->
   try global:send(Name, {'$gen_cast', Msg}) of
      _ -> ok
   catch
      _:_ -> ok
   end;
cast({via, RegMod, Name}, Msg) ->
   try RegMod:send(Name, {'$gen_cast', Msg}) of
      _ -> ok
   catch
      _:_ -> ok
   end;
cast({Name, Node} = Dest, Msg) when is_atom(Name), is_atom(Node) ->
   try
      erlang:send(Dest, {'$gen_cast', Msg}),
      ok
   catch
      error:_ -> ok
   end;
cast(Dest, Msg) ->
   try
      erlang:send(Dest, {'$gen_cast', Msg}),
      ok
   catch
      error:_ -> ok
   end.

%% 异步广播，不返回任何内容，只是发送“ n”祈祷
abcast(Name, Msg) when is_atom(Name) ->
   doAbcast([node() | nodes()], Name, Msg).

abcast(Nodes, Name, Msg) when is_list(Nodes), is_atom(Name) ->
   doAbcast(Nodes, Name, Msg).

doAbcast([Node | Nodes], Name, Msg) when is_atom(Node) ->
   try
      erlang:send({Name, Node}, {'$gen_cast', Msg}),
      ok
   catch
      error:_ -> ok
   end,
   doAbcast(Nodes, Name, Msg);
doAbcast([], _, _) -> abcast.

%% Reply from a status machine callback to whom awaits in call/2
-spec reply([replyAction()] | replyAction()) -> ok.
reply({reply, From, Reply}) ->
   reply(From, Reply);
reply(Replies) when is_list(Replies) ->
   replies(Replies).

replies([{reply, From, Reply} | Replies]) ->
   reply(From, Reply),
   replies(Replies);
replies([]) ->
   ok.

-compile({inline, [reply/2]}).
-spec reply(From :: from(), Reply :: term()) -> ok.
reply({To, Tag}, Reply) ->
   Msg = {Tag, Reply},
   try To ! Msg of
      _ ->
         ok
   catch
      _:_ -> ok
   end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% API helpers  end  %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% gen_event  start %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
epmRequest({global, Name}, Msg) ->
   try global:send(Name, Msg) of
      _ -> ok
   catch
      _:_ -> ok
   end;
epmRequest({via, RegMod, Name}, Msg) ->
   try RegMod:send(Name, Msg) of
      _ -> ok
   catch
      _:_ -> ok
   end;
epmRequest(EpmSrv, Cmd) ->
   EpmSrv ! Cmd,
   ok.

-spec epmInfo(serverRef(), epmHandler(), term()) -> term().
epmInfo(EpmSrv, EpmHandler, Msg) ->
   epmRequest(EpmSrv, {'$epm_info', EpmHandler, Msg}).

-spec infoNotify(serverRef(), term()) -> 'ok'.
infoNotify(EpmSrv, Event) ->
   epmRequest(EpmSrv, {'$epm_info', '$infoNotify', Event}).

epmRpc(EpmSrv, Cmd) ->
   try gen:call(EpmSrv, '$epm_call', Cmd, infinity) of
      {ok, Reply} ->
         Reply
   catch
      Class:Reason:Stacktrace ->
         erlang:raise(Class, {Reason, {?MODULE, call, [EpmSrv, Cmd, infinity]}}, Stacktrace)
   end.

epmRpc(EpmSrv, Cmd, Timeout) ->
   try gen:call(EpmSrv, '$epm_call', Cmd, Timeout) of
      {ok, Reply} ->
         Reply
   catch
      Class:Reason:Stacktrace ->
         erlang:raise(Class, {Reason, {?MODULE, call, [EpmSrv, Cmd, Timeout]}}, Stacktrace)
   end.

-spec callNotify(serverRef(), term()) -> 'ok'.
callNotify(EpmSrv, Event) ->
   epmRpc(EpmSrv, {'$syncNotify', Event}).

-spec epmCall(serverRef(), epmHandler(), term()) -> term().
epmCall(EpmSrv, EpmHandler, Query) ->
   epmRpc(EpmSrv, {'$epmCall', EpmHandler, Query}).

-spec epmCall(serverRef(), epmHandler(), term(), timeout()) -> term().
epmCall(EpmSrv, EpmHandler, Query, Timeout) ->
   epmRpc(EpmSrv, {'$epmCall', EpmHandler, Query}, Timeout).

-spec addEpm(serverRef(), epmHandler(), term()) -> term().
addEpm(EpmSrv, EpmHandler, Args) ->
   epmRpc(EpmSrv, {'$addEpm', EpmHandler, Args}).

-spec addSupEpm(serverRef(), epmHandler(), term()) -> term().
addSupEpm(EpmSrv, EpmHandler, Args) ->
   epmRpc(EpmSrv, {'$addSupEpm', EpmHandler, Args}).

-spec deleteEpm(serverRef(), epmHandler(), term()) -> term().
deleteEpm(EpmSrv, EpmHandler, Args) ->
   epmRpc(EpmSrv, {'$deleteEpm', EpmHandler, Args}).

-spec swapEpm(serverRef(), {epmHandler(), term()}, {epmHandler(), term()}) -> 'ok' | {'error', term()}.
swapEpm(EpmSrv, {H1, A1}, {H2, A2}) ->
   epmRpc(EpmSrv, {'$swapEpm', H1, A1, H2, A2}).

-spec swapSupEpm(serverRef(), {epmHandler(), term()}, {epmHandler(), term()}) -> 'ok' | {'error', term()}.
swapSupEpm(EpmSrv, {H1, A1}, {H2, A2}) ->
   epmRpc(EpmSrv, {'$swapSupEpm', H1, A1, H2, A2, self()}).

-spec whichEpm(serverRef()) -> [epmHandler()].
whichEpm(EpmSrv) ->
   epmRpc(EpmSrv, '$which_handlers').

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% EPM inner fun
addNewEpm(InitRet, Module, EpmId, EpmSup) ->
   case InitRet of
      {ok, State} ->
         EpmHer = #epmHer{epmM = Module, epmId = EpmId, epmS = State},
         OldList = getEpmList(),
         setEpmHer(EpmHer),
         setEpmList([{EpmId, Module, EpmSup} | OldList]),
         {ok, false};
      {ok, State, hibernate} ->
         EpmHer = #epmHer{epmM = Module, epmId = EpmId, epmS = State},
         OldList = getEpmList(),
         setEpmHer(EpmHer),
         setEpmList([{EpmId, Module, EpmSup} | OldList]),
         {ok, true};
      Other ->
         {Other, false}
   end.

doAddEpm({Module, EpmId}, Args, EpmSup) ->
   case getEpmHer(EpmId) of
      undefined ->
         try Module:init(Args) of
            Result ->
               addNewEpm(Result, Module, EpmId, EpmSup)
         catch
            throw:Ret ->
               addNewEpm(Ret, Module, EpmId, EpmSup);
            C:R:T ->
               {{C, R, T}, false}
         end;
      _ ->
         {existed, false}
   end;
doAddEpm(Module, Args, EpmSup) ->
   case getEpmHer(Module) of
      undefined ->
         try Module:init(Args) of
            Result ->
               addNewEpm(Result, Module, Module, EpmSup)
         catch
            throw:Ret ->
               addNewEpm(Ret, Module, Module, EpmSup);
            C:R:T ->
               {{C, R, T}, false}
         end;
      _ ->
         {existed, false}
   end.

doAddSupEpm(Module, Args, EpmSup) ->
   case doAddEpm(Module, Args, EpmSup) of
      {ok, _} = Result ->
         link(EpmSup),
         Result;
      Ret ->
         Ret
   end.

doSwapEpm(EpmId1, Args1, EpmMId, Args2) ->
   case getEpmHer(EpmId1) of
      undefined ->
         doAddEpm(EpmMId, {Args2, undefined}, false);
      #epmHer{epmSup = EpmSup} = EpmHer ->
         State2 = epmTerminate(EpmHer, Args1, swapped, {swapped, EpmMId, EpmSup}),
         case EpmSup of
            false ->
               doAddEpm(EpmMId, {Args2, State2}, false);
            _ ->
               doAddSupEpm(EpmMId, {Args2, State2}, EpmSup)
         end
   end.

doSwapSupEpm(EpmId1, Args1, EpmMId, Args2, EpmSup) ->
   case getEpmHer(EpmId1) of
      undefined ->
         doAddSupEpm(EpmMId, {Args2, undefined}, EpmSup);
      EpmHer ->
         State2 = epmTerminate(EpmHer, Args1, swapped, {swapped, EpmMId, EpmSup}),
         doAddSupEpm(EpmMId, {Args2, State2}, EpmSup)
   end.

doNotify([{EpmId, _EmpM} | T], Event, Func, IsHib) ->
   case doEpmHandle(getEpmHer(EpmId), Func, Event, false) of
      ok ->
         doNotify(T, Event, Func, IsHib);
      hibernate ->
         doNotify(T, Event, Func, true);
      _Other ->
         doNotify(T, Event, Func, IsHib)
   end;
doNotify([], _Event, _Func, IsHib) ->
   IsHib.

doEpmHandle(#epmHer{epmM = EpmM, epmS = EpmS} = EpmHer, Func, Event, From) ->
   try EpmM:Func(Event, EpmS) of
      Result ->
         handleEpmCallbackRet(Result, EpmHer, Event, From)
   catch
      throw:Ret ->
         handleEpmCallbackRet(Ret, EpmHer, Event, From);
      C:R:S ->
         epmTerminate(EpmHer, {error, {C, R, S}}, Event, crash)
   end;
doEpmHandle(undefined, _Func, _Event, _From) ->
   no_epm.

doDeleteEpm(EpmId, Args) ->
   case getEpmHer(EpmId) of
      undefined ->
         {error, module_not_found};
      EpmHer ->
         epmTerminate(EpmHer, Args, delete, normal)
   end.

proTerminate(#epmHer{epmM = EpmM, epmS = State} = EpmHer, Args, LastIn, Reason) ->
   case erlang:function_exported(EpmM, terminate, 2) of
      true ->
         Res = (catch EpmM:terminate(Args, State)),
         reportTerminate(EpmHer, Reason, Args, LastIn, Res),
         Res;
      false ->
         reportTerminate(EpmHer, Reason, Args, LastIn, ok),
         ok
   end.

epmTerminate(#epmHer{epmM = EpmM, epmId = EpmId, epmS = State} = EpmHer, Args, LastIn, Reason) ->
   %% 删除列表的数据
   OldList = getEpmList(),
   NewList = lists:keydelete(EpmId, 1, OldList),
   setEpmList(NewList),
   %% 删除进程字典中的数据
   delEpmHer(EpmId),

   case erlang:function_exported(EpmM, terminate, 2) of
      true ->
         Res = (catch EpmM:terminate(Args, State)),
         reportTerminate(EpmHer, Reason, Args, LastIn, Res),
         Res;
      false ->
         reportTerminate(EpmHer, Reason, Args, LastIn, ok),
         ok
   end.

reportTerminate(EpmHer, crash, {error, Why}, LastIn, _) ->
   reportTerminate2(EpmHer, Why, LastIn);
%% How == normal | shutdown | {swapped, NewHandler, NewSupervisor}
reportTerminate(EpmHer, How, _, LastIn, _) ->
   reportTerminate2(EpmHer, How, LastIn).

reportTerminate2(#epmHer{epmSup = EpmSup, epmId = EpmId, epmS = State} = EpmHer, Reason, LastIn) ->
   report_error(EpmHer, Reason, State, LastIn),
   case EpmSup of
      false ->
         ok;
      _ ->
         EpmSup ! {gen_event_EXIT, EpmId, Reason},
         ok
   end.

report_error(_EpmHer, normal, _, _) -> ok;
report_error(_EpmHer, shutdown, _, _) -> ok;
report_error(_EpmHer, {swapped, _, _}, _, _) -> ok;
report_error(#epmHer{epmM = EpmM, epmId = EpmId}, Reason, State, LastIn) ->
   ?LOG_ERROR(
      #{
         label=>{gen_ipc, epm_terminate},
         handler => {EpmId, EpmM},
         name => getProName(),
         last_message => LastIn,
         state=> format_status(terminate, [EpmM, get(), State]),
         reason => Reason
      },
      #{
         domain => [otp],
         report_cb => fun gen_ipc:epm_log/1,
         error_logger => #{tag => error}
      }).

epm_log(#{label := {gen_ipc, epm_terminate}, handler := Handler, name := SName, last_message := LastIn, state := State, reason := Reason}) ->
   Reason1 =
      case Reason of
         {'EXIT', {undef, [{M, F, A, L} | MFAs]}} ->
            case code:is_loaded(M) of
               false ->
                  {'module could not be loaded', [{M, F, A, L} | MFAs]};
               _ ->
                  case erlang:function_exported(M, F, length(A)) of
                     true ->
                        {undef, [{M, F, A, L} | MFAs]};
                     false ->
                        {'function not exported', [{M, F, A, L} | MFAs]}
                  end
            end;
         {'EXIT', Why} ->
            Why;
         _ ->
            Reason
      end,
   {"** gen_ipc emp handler ~p crashed.~n"
   "** Was installed in ~tp~n"
   "** Last event was: ~tp~n"
   "** When handler state == ~tp~n"
   "** Reason == ~tp~n", [Handler, SName, LastIn, State, Reason1]};
epm_log(#{label := {gen_ipc, no_handle_info}, module := Mod, message := Msg}) ->
   {"** Undefined handle_info in ~tp~n"
   "** Unhandled message: ~tp~n", [Mod, Msg]}.

epmStopAll() ->
   EpmList = getEpmList(),
   [
      begin
         proTerminate(getEpmHer(EpmId), stop, 'receive', shutdown),
         case EpmSup =/= false of
            true ->
               unlink(EpmSup);
            _ ->
               ignore
         end
      end || {EpmId, _EpmM, EpmSup} <- EpmList
   ].

epmStopOne(ExitEmpSup) ->
   EpmList = getEpmList(),
   [epmTerminate(getEpmHer(EpmId), stop, 'receive', shutdown) || {EpmId, _EpmM, EpmSup} <- EpmList, ExitEmpSup =:= EpmSup].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% gen_event  end %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 仅在＃params.parent不同时更新。今天，这应该是不可能的（OTP-22.0），但是，例如，如果有人在新的sys调用中实现了更改服务器父服务器，则可能发生这种情况。
-compile({inline, updateParent/1}).
updateParent(Parent) ->
   case getParent() of
      Parent ->
         ignore;
      _ ->
         setParent(Parent)
   end.
%%%==========================================================================
%%% Internal callbacks
wakeup_from_hibernate(CycleData, CurStatus, CurState, Debug, IsHibernate) ->
   %% 这是一条新消息，唤醒了我们，因此我们必须立即收到它
   receiveMsgWait(CycleData, CurStatus, CurState, Debug, IsHibernate).

%%%==========================================================================
%% Entry point for system_continue/3
reLoopEntry(CycleData, CurStatus, CurState, Debug, IsHibernate) ->
   if
      IsHibernate ->
         proc_lib:hibernate(?MODULE, wakeup_from_hibernate, [CycleData, CurStatus, CurState, Debug, IsHibernate]);
      true ->
         receiveMsgWait(CycleData, CurStatus, CurState, Debug, IsHibernate)
   end.

%% 接收新的消息
receiveMsgWait(#cycleData{hibernateAfter = HibernateAfterTimeout} = CycleData, CurStatus, CurState, Debug, IsHibernate) ->
   receive
      Msg ->
         case Msg of
            {'$gen_call', From, Request} ->
               matchCallMsg(CycleData, CurStatus, CurState, Debug, {{call, From}, Request});
            {'$gen_cast', Cast} ->
               matchCastMsg(CycleData, CurStatus, CurState, Debug, {cast, Cast});
            {timeout, TimerRef, TimeoutType} ->
               case element(#cycleData.timers, CycleData) of
                  #{TimeoutType := {TimerRef, TimeoutMsg}} = Timers ->
                     NewTimer = maps:remove(TimeoutType, Timers),
                     NewCycleData = setelement(#cycleData.timers, CycleData, NewTimer),
                     matchTimeoutMsg(NewCycleData, CurStatus, CurState, Debug, {TimeoutType, TimeoutMsg});
                  #{} ->
                     matchInfoMsg(CycleData, CurStatus, CurState, Debug, {info, Msg})
               end;
            {system, PidFrom, Request} ->
               %% 不返回但尾递归调用 system_continue/3
               sys:handle_system_msg(Request, PidFrom, getParent(), ?MODULE, Debug, {CycleData, CurStatus, CurState, IsHibernate});
            {'EXIT', PidFrom, Reason} ->
               case getParent() of
                  PidFrom ->
                     terminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, []);
                  _ ->
                     epmStopOne(PidFrom),
                     matchInfoMsg(CycleData, CurStatus, CurState, Debug, {info, Msg})
               end;
            {'$epm_call', From, Request} ->
               matchEpmCallMsg(CycleData, CurStatus, CurState, Debug, From, Request);
            {'$epm_info', CmdOrEmpHandler, Event} ->
               matchEpmInfoMsg(CycleData, CurStatus, CurState, Debug, CmdOrEmpHandler, Event);
            _ ->
               matchInfoMsg(CycleData, CurStatus, CurState, Debug, {info, Msg})
         end
   after
      HibernateAfterTimeout ->
         proc_lib:hibernate(?MODULE, wakeup_from_hibernate, [CycleData, CurStatus, CurState, Debug, IsHibernate])
   end.

matchCallMsg(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, {{call, From}, Request} = Event) ->
   NewDebug = ?SYS_DEBUG(Debug, {in, Event, CurStatus}),
   try Module:handleCall(Request, CurStatus, CurState, From) of
      Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, From)
   catch
      throw:Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, From);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, NewDebug, [Event])
   end.

matchCastMsg(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, {cast, Cast} = Event) ->
   NewDebug = ?SYS_DEBUG(Debug, {in, Event, CurStatus}),
   try Module:handleCast(Cast, CurStatus, CurState) of
      Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false)
   catch
      throw:Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, NewDebug, [Event])
   end.

matchInfoMsg(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, {info, Msg} = Event) ->
   NewDebug = ?SYS_DEBUG(Debug, {in, Event, CurStatus}),
   try Module:handleInfo(Msg, CurStatus, CurState) of
      Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false)
   catch
      throw:Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, NewDebug, [Event])
   end.

matchTimeoutMsg(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, {TimeoutType, TimeoutMsg} = Event) ->
   NewDebug = ?SYS_DEBUG(Debug, {in, Event, CurStatus}),
   try Module:handleOnevent(TimeoutType, TimeoutMsg, CurStatus, CurState) of
      Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false)
   catch
      throw:Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, NewDebug, [Event])
   end.

matchEpmCallMsg(CycleData, CurStatus, CurState, Debug, From, Request) ->
   NewDebug = ?SYS_DEBUG(Debug, {in, Request, CurStatus}),
   case Request of
      '$which_handlers' ->
         reply(From, getEpmList());
      {'$addEpm', EpmHandler, Args} ->
         {Reply, IsHib} = doAddEpm(EpmHandler, Args, false),
         reply(From, Reply),
         reLoopEntry(CycleData, CurStatus, CurState, NewDebug, IsHib);
      {'$addSupEpm', EpmHandler, Args, EpmSup} ->
         {Reply, IsHib} = doAddSupEpm(EpmHandler, Args, EpmSup),
         reply(From, Reply),
         reLoopEntry(CycleData, CurStatus, CurState, NewDebug, IsHib);
      {'$deleteEpm', EpmHandler, Args} ->
         Reply = doDeleteEpm(EpmHandler, Args),
         reply(From, Reply),
         receiveMsgWait(CycleData, CurStatus, CurState, NewDebug, false);
      {'$swapEpm', EpmId1, Args1, EpmId2, Args2} ->
         {Reply, IsHib} = doSwapEpm(EpmId1, Args1, EpmId2, Args2),
         reply(From, Reply),
         reLoopEntry(CycleData, CurStatus, CurState, NewDebug, IsHib);
      {'$swapSupEpm', EpmId1, Args1, EpmId2, Args2, SupPid} ->
         {Reply, IsHib} = doSwapSupEpm(EpmId1, Args1, EpmId2, Args2, SupPid),
         reply(From, Reply),
         reLoopEntry(CycleData, CurStatus, CurState, NewDebug, IsHib);
      {'$syncNotify', Event} ->
         IsHib = doNotify(getEpmList(), Event, handleEvent, false),
         reply(From, ok),
         startEpmCall(CycleData, CurStatus, CurState, NewDebug, handleEpmEvent, Request, IsHib);
      {'$epmCall', EpmHandler, Query} ->
         IsHib = doEpmHandle(getEpmHer(EpmHandler), handleCall, Query, From),
         startEpmCall(CycleData, CurStatus, CurState, Debug, handleEpmCall, Request, IsHib)
   end.

matchEpmInfoMsg(CycleData, CurStatus, CurState, Debug, CmdOrEmpHandler, Event) ->
   case CmdOrEmpHandler of
      '$infoNotify' ->
         IsHib = doNotify(getEpmList(), Event, handleEvent, false),
         startEpmCall(CycleData, CurStatus, CurState, Debug, handleEpmEvent, Event, IsHib);
      EpmHandler ->
         IsHib = doEpmHandle(getEpmHer(EpmHandler), Event, handleInfo, false),
         startEpmCall(CycleData, CurStatus, CurState, Debug, handleEpmInfo, Event, IsHib)
   end.

startEpmCall(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, CallbackFun, Event, IsHib) ->
   case erlang:function_exported(Module, CallbackFun, 3) of
      true ->
         NewDebug = ?SYS_DEBUG(Debug, {in, Event, CurStatus}),
         try Module:CallbackFun(Event, CurStatus, CurState) of
            Result ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Result, ?CB_FORM_EVENT, false)
         catch
            throw:Ret ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, NewDebug, [Event], Ret, ?CB_FORM_EVENT, false);
            Class:Reason:Stacktrace ->
               terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, NewDebug, [Event])
         end;
      _ ->
         receiveMsgWait(CycleData, CurStatus, CurState, Debug, IsHib)
   end.

startEnterCall(#cycleData{module = Module} = CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter) ->
   try Module:handleEnter(PrevStatus, CurStatus, CurState) of
      Result ->
         handleEnterCallbackRet(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, Result)
   catch
      throw:Result ->
         handleEnterCallbackRet(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, Result);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, CycleData, Debug, CurStatus, CurState, LeftEvents)
   end.

startAfterCall(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, LeftEvents, Args) ->
   try Module:handleAfter(Args, CurStatus, CurState) of
      Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, ?CB_FORM_AFTER, false)
   catch
      throw:Result ->
         handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, ?CB_FORM_AFTER, false);
      Class:Reason:Stacktrace ->
         terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents)
   end.

startEventCall(#cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, LeftEvents, {Type, Content}) ->
   case Type of
      'cast' ->
         try Module:handleCast(Content, CurStatus, CurState) of
            Result ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, ?CB_FORM_EVENT, false)
         catch
            throw:Ret ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Ret, ?CB_FORM_EVENT, false);
            Class:Reason:Stacktrace ->
               terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents)
         end;
      'info' ->
         try Module:handleInfo(Content, CurStatus, CurState) of
            Result ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, ?CB_FORM_EVENT, false)
         catch
            throw:Ret ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Ret, ?CB_FORM_EVENT, false);
            Class:Reason:Stacktrace ->
               terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents)
         end;
      {'call', From} ->
         try Module:handleCall(Content, CurStatus, CurState, From) of
            Result ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, ?CB_FORM_EVENT, From)
         catch
            throw:Ret ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Ret, ?CB_FORM_EVENT, From);
            Class:Reason:Stacktrace ->
               terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents)
         end;
      _ ->
         try Module:handleOnevent(Type, Content, CurStatus, CurState) of
            Result ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, ?CB_FORM_EVENT, false)
         catch
            throw:Ret ->
               handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Ret, ?CB_FORM_EVENT, false);
            Class:Reason:Stacktrace ->
               terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents)
         end
   end.

handleEpmCallbackRet(Result, EpmHer, Event, From) ->
   case Result of
      ok ->
         ok;
      {ok, NewEpmS} ->
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         setEpmHer(MewEpmHer),
         ok;
      {ok, NewEpmS, hibernate} ->
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         setEpmHer(MewEpmHer),
         hibernate;
      {swapEpm, NewEpmS, Args1, EpmMId, Args2} ->
         #epmHer{epmId = OldEpmMId, epmSup = EpmSup} = MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         State = epmTerminate(MewEpmHer, Args1, swapped, {swapped, OldEpmMId, EpmSup}),
         case EpmSup of
            false ->
               doAddEpm(EpmMId, {Args2, State}, false);
            _ ->
               doAddSupEpm(EpmMId, {Args2, State}, EpmSup)
         end,
         ok;
      {swapEpm, Reply, NewEpmS, Args1, EpmMId, Args2} ->
         reply(From, Reply),
         #epmHer{epmId = OldEpmMId, epmSup = EpmSup} = MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         State = epmTerminate(MewEpmHer, Args1, swapped, {swapped, OldEpmMId, EpmSup}),
         case EpmSup of
            false ->
               doAddEpm(EpmMId, {Args2, State}, false);
            _ ->
               doAddSupEpm(EpmMId, {Args2, State}, EpmSup)
         end,
         ok;
      removeEpm ->
         epmTerminate(EpmHer, removeEpm, remove, normal),
         ok;
      {removeEpm, Reply} ->
         reply(From, Reply),
         epmTerminate(EpmHer, removeEpm, remove, normal),
         ok;
      {reply, Reply} ->
         reply(From, Reply),
         ok;
      {reply, Reply, NewEpmS} ->
         reply(From, Reply),
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         setEpmHer(MewEpmHer),
         ok;
      {reply, Reply, NewEpmS, hibernate} ->
         reply(From, Reply),
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         setEpmHer(MewEpmHer),
         hibernate;
      Other ->
         epmTerminate(EpmHer, {error, Other}, Event, crash),
         ok
   end.

handleEnterCallbackRet(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, Result) ->
   case Result of
      {keepStatus, NewState} ->
         dealEnterCallbackRet(CycleData, PrevStatus, NewState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, false);
      {keepStatus, NewState, Actions} ->
         parseEnterActionsList(CycleData, PrevStatus, NewState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, false, Actions);
      keepStatusState ->
         dealEnterCallbackRet(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, false);
      {keepStatusState, Actions} ->
         parseEnterActionsList(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, false, Actions);
      {repeatStatus, NewState} ->
         dealEnterCallbackRet(CycleData, PrevStatus, NewState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, true);
      {repeatStatus, NewState, Actions} ->
         parseEnterActionsList(CycleData, PrevStatus, NewState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, true, Actions);
      repeatStatusState ->
         dealEnterCallbackRet(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, true);
      {repeatStatusState, Actions} ->
         parseEnterActionsList(CycleData, PrevStatus, CurState, CurStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, true, Actions);
      stop ->
         terminate(exit, normal, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents);
      {stop, Reason} ->
         terminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents);
      {stop, Reason, NewState} ->
         terminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, NewState, Debug, LeftEvents);
      {stopReply, Reason, Replies} ->
         replyThenTerminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents, Replies);
      {stopReply, Reason, Replies, NewState} ->
         replyThenTerminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, NewState, Debug, LeftEvents, Replies);
      _ ->
         terminate(error, {badReturnFrom_EnterFunction, Result}, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents)
   end.

handleEventCallbackRet(CycleData, CurStatus, CurState, Debug, LeftEvents, Result, CallbackForm, From) ->
   case Result of
      {noreply, NewState} ->
         receiveMsgWait(CycleData, CurStatus, NewState, Debug, false);
      {noreply, NewState, Option} ->
         case Option of
            hibernate ->
               reLoopEntry(CycleData, CurStatus, NewState, Debug, true);
            {doAfter, Args} ->
               startAfterCall(CycleData, CurStatus, NewState, Debug, [], Args);
            _Ret ->
               terminate(error, {bad_reply_option, _Ret}, ?STACKTRACE(), CycleData, CurStatus, NewState, Debug, [])
         end;
      {nextStatus, NewStatus, NewState} ->
         dealEventCallbackRet(CycleData, CurStatus, NewState, NewStatus, Debug, LeftEvents, NewStatus =/= CurStatus);
      {nextStatus, NewStatus, NewState, Actions} ->
         parseEventActionsList(CycleData, CurStatus, NewState, NewStatus, Debug, LeftEvents, NewStatus =/= CurStatus, Actions, CallbackForm);
      {keepStatus, NewState} ->
         dealEventCallbackRet(CycleData, CurStatus, NewState, CurStatus, Debug, LeftEvents, false);
      {keepStatus, NewState, Actions} ->
         parseEventActionsList(CycleData, CurStatus, NewState, CurStatus, Debug, LeftEvents, false, Actions, CallbackForm);
      keepStatusState ->
         dealEventCallbackRet(CycleData, CurStatus, CurState, CurStatus, Debug, LeftEvents, false);
      {keepStatusState, Actions} ->
         parseEventActionsList(CycleData, CurStatus, CurState, CurStatus, Debug, LeftEvents, false, Actions, CallbackForm);
      {repeatStatus, NewState} ->
         dealEventCallbackRet(CycleData, CurStatus, NewState, CurStatus, Debug, LeftEvents, true);
      {repeatStatus, NewState, Actions} ->
         parseEventActionsList(CycleData, CurStatus, NewState, CurStatus, Debug, LeftEvents, true, Actions, CallbackForm);
      repeatStatusState ->
         dealEventCallbackRet(CycleData, CurStatus, CurState, CurStatus, Debug, LeftEvents, true);
      {repeatStatusState, Actions} ->
         parseEventActionsList(CycleData, CurStatus, CurState, CurStatus, Debug, LeftEvents, true, Actions, CallbackForm);
      stop ->
         terminate(exit, normal, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents);
      {stop, Reason} ->
         terminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents);
      {stop, Reason, NewState} ->
         terminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, NewState, Debug, LeftEvents);
      {stopReply, Reason, Replies} ->
         replyThenTerminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents, Replies);
      {stopReply, Reason, Replies, NewState} ->
         replyThenTerminate(exit, Reason, ?STACKTRACE(), CycleData, CurStatus, NewState, Debug, LeftEvents, Replies);
      {reply, Reply, NewState} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, {out, Reply, From}),
         receiveMsgWait(CycleData, CurStatus, NewState, NewDebug, false);
      {reply, Reply, NewState, Option} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, {out, Reply, From}),
         case Option of
            hibernate ->
               reLoopEntry(CycleData, CurStatus, NewState, NewDebug, true);
            {doAfter, Args} ->
               startAfterCall(CycleData, CurStatus, NewState, NewDebug, [], Args);
            _Ret ->
               terminate(error, {bad_reply_option, _Ret}, ?STACKTRACE(), CycleData, CurStatus, NewState, Debug, [])
         end;
      {sreply, Reply, NewStatus, NewState} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, {out, Reply, From}),
         dealEventCallbackRet(CycleData, CurStatus, NewState, NewStatus, NewDebug, LeftEvents, NewStatus =/= CurStatus);
      {sreply, Reply, NewStatus, NewState, Actions} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, {out, Reply, From}),
         parseEventActionsList(CycleData, CurStatus, NewState, NewStatus, NewDebug, LeftEvents, NewStatus =/= CurStatus, Actions, CallbackForm);
      _ ->
         terminate(error, {bad_return_from_status_function, Result}, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, LeftEvents)
   end.

-compile({inline, [dealEnterCallbackRet/12]}).
dealEnterCallbackRet(#cycleData{isEnter = IsEnter} = CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter, IsCallEnter) ->
   case IsEnter andalso IsCallEnter of
      true ->
         startEnterCall(CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter);
      false ->
         performTransitions(CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter)
   end.

-compile({inline, [dealEventCallbackRet/7]}).
dealEventCallbackRet(#cycleData{isEnter = IsEnter} = CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, IsCallEnter) ->
   case IsEnter andalso IsCallEnter of
      true ->
         startEnterCall(CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, [], [], false, false, false);
      false ->
         performTransitions(CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, [], [], false, false, false)
   end.

%% 处理enter callback 动作列表
parseEnterActionsList(CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, Timeouts, NextEvents, IsPostpone, IsCallEnter, IsHibernate, DoAfter, Actions) ->
   %% enter 调用不能改成状态 actions 不能返回 IsPostpone = true 但是可以取消之前的推迟  设置IsPostpone = false 不能设置 doafter 不能插入事件
   case loopParseActionsList(Actions, ?CB_FORM_ENTER, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents) of
      {error, ErrorContent} ->
         terminate(error, ErrorContent, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, []);
      {NewCycleData, NewDebug, NewIsPostpone, NewIsHibernate, DoAfter, NewTimeouts, NextEvents} ->
         case IsCallEnter andalso element(#cycleData.isEnter, NewCycleData) of
            true ->
               startEnterCall(NewCycleData, CurStatus, CurState, NewStatus, NewDebug, LeftEvents, NewTimeouts, NextEvents, NewIsPostpone, NewIsHibernate, DoAfter);
            _ ->
               performTransitions(NewCycleData, CurStatus, CurState, NewStatus, NewDebug, LeftEvents, NewTimeouts, NextEvents, NewIsPostpone, NewIsHibernate, DoAfter)
         end
   end.

%% 处理非 enter 或者after callback 返回的动作列表
parseEventActionsList(CycleData, CurStatus, CurState, NewStatus, Debug, LeftEvents, IsCallEnter, Actions, CallbackForm) ->
   case loopParseActionsList(Actions, CallbackForm, CycleData, Debug, false, false, false, [], []) of
      {error, ErrorContent} ->
         terminate(error, ErrorContent, ?STACKTRACE(), CycleData, CurStatus, CurState, Debug, []);
      {NewCycleData, NewDebug, NewIsPostpone, NewIsHibernate, MewDoAfter, NewTimeouts, NewNextEvents} ->
         case IsCallEnter andalso element(#cycleData.isEnter, NewCycleData) of
            true ->
               startEnterCall(NewCycleData, CurStatus, CurState, NewStatus, NewDebug, LeftEvents, NewTimeouts, NewNextEvents, NewIsPostpone, NewIsHibernate, MewDoAfter);
            _ ->
               performTransitions(NewCycleData, CurStatus, CurState, NewStatus, NewDebug, LeftEvents, NewTimeouts, NewNextEvents, NewIsPostpone, NewIsHibernate, MewDoAfter)
         end
   end.

loopParseActionsList([], _CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents) ->
   {CycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents};
loopParseActionsList([OneAction | LeftActions], CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents) ->
   case OneAction of
      {reply, From, Reply} ->
         reply(From, Reply),
         NewDebug = ?SYS_DEBUG(Debug, {out, Reply, From}),
         loopParseActionsList(LeftActions, CallbackForm, CycleData, NewDebug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents);
      {eTimeout, _Time, _TimeoutMsg, _Options} = ENewTVOpt ->
         case checkTimeOptions(ENewTVOpt) of
            error_timeout_opt ->
               {error, {bad_eTimeout_action, ENewTVOpt}};
            RetTV ->
               loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [RetTV | Timeouts], NextEvents)
         end;
      {sTimeout, _Time, _TimeoutMsg, _Options} = SNewTVOpt ->
         case checkTimeOptions(SNewTVOpt) of
            error_timeout_opt ->
               {error, {bad_sTimeout_action, SNewTVOpt}};
            RetTV ->
               loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [RetTV | Timeouts], NextEvents)
         end;
      {{gTimeout, _Name}, _Time, _TimeoutMsg, _Options} = GNewTVOpt ->
         case checkTimeOptions(GNewTVOpt) of
            error_timeout_opt ->
               {error, {bad_gTimeout_action, GNewTVOpt}};
            RetTV ->
               loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [RetTV | Timeouts], NextEvents)
         end;
      {u_eTimeout, _TimeoutMsg} = UENewTV ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [UENewTV | Timeouts], NextEvents);
      {u_sTimeout, _TimeoutMsg} = USNewTV ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [USNewTV | Timeouts], NextEvents);
      {{u_gTimeout, _Name}, _TimeoutMsg} = UGNewTV ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [UGNewTV | Timeouts], NextEvents);
      c_eTimeout ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [c_eTimeout | Timeouts], NextEvents);
      c_sTimeout ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [c_sTimeout | Timeouts], NextEvents);
      {c_gTimeout, _Name} = CGNewTV ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [CGNewTV | Timeouts], NextEvents);
      {isEnter, IsEnter} when is_boolean(IsEnter) ->
         case element(#cycleData.isEnter, CycleData) of
            IsEnter ->
               loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents);
            _ ->
               NewCycleData = setelement(#cycleData.isEnter, CycleData, IsEnter),
               loopParseActionsList(LeftActions, CallbackForm, NewCycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents)
         end;
      {isHibernate, NewIsHibernate} when is_boolean(NewIsHibernate) ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, NewIsHibernate, DoAfter, Timeouts, NextEvents);
      {isPostpone, NewIsPostpone} when is_boolean(NewIsPostpone) andalso (not NewIsPostpone orelse CallbackForm == ?CB_FORM_EVENT) ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, NewIsPostpone, IsHibernate, DoAfter, Timeouts, NextEvents);
      {doAfter, Args} when CallbackForm == ?CB_FORM_EVENT ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, {true, Args}, Timeouts, NextEvents);
      {eTimeout, Time, _TimeoutMsg} = ENewTV when ?REL_TIMEOUT(Time) ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [ENewTV | Timeouts], NextEvents);
      {sTimeout, Time, _TimeoutMsg} = SNewTV when ?REL_TIMEOUT(Time) ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [SNewTV | Timeouts], NextEvents);
      {{gTimeout, _Name}, Time, _TimeoutMsg} = GNewTV when ?REL_TIMEOUT(Time) ->
         loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, [GNewTV | Timeouts], NextEvents);
      {nextEvent, Type, Content} when CallbackForm == ?CB_FORM_EVENT orelse CallbackForm == ?CB_FORM_AFTER ->
         %% 处理next_event动作
         case isEventType(Type) of
            true ->
               loopParseActionsList(LeftActions, CallbackForm, CycleData, Debug, IsPostpone, IsHibernate, DoAfter, Timeouts, [{Type, Content} | NextEvents]);
            _ ->
               {error, {bad_next_event, Type, Content}}
         end;
      _ActRet ->
         {error, {bad_action_type, _ActRet}}
   end.

checkTimeOptions({TimeoutType, Time, TimeoutMsg, Options} = NewTV) ->
   case Options of
      [{abs, true}] when ?ABS_TIMEOUT(Time) ->
         NewTV;
      [{abs, false}] when ?REL_TIMEOUT(Time) ->
         {TimeoutType, Time, TimeoutMsg};
      [] when ?REL_TIMEOUT(Time) ->
         {TimeoutType, Time, TimeoutMsg};
      _ ->
         %% 如果将来 start_timer opt扩展了 这里的代码也要修改
         error_timeout_opt
   end.

%% 进行状态转换
performTransitions(#cycleData{postponed = Postponed, timers = Timers} = CycleData, CurStatus, CurState, NewStatus, Debug, [Event | LeftEvents], Timeouts, NextEvents, IsPostpone, IsHibernate, DoAfter) ->
   %% 已收集所有选项，并缓冲next_events。执行实际状态转换。如果推迟则将当前事件移至推迟
   %% 此时 Timeouts, NextEvents的顺序与最开始出现的顺序相反. 后面执行的顺序 超时添加和更新 + 零超时 + 当前事件 + 反序的Postpone事件 + LeftEvent  %% TODO 测试验证
   NewDebug = ?SYS_DEBUG(Debug, case IsPostpone of true -> {postpone, Event, CurStatus, NewStatus}; _ -> {consume, Event, NewStatus, NewStatus} end),
   if
      CurStatus =:= NewStatus ->
         %% Cancel event timeout
         LastTimers =
            case Timers of
               #{eTimeout := {TimerRef, _TimeoutMsg}} ->
                  cancelTimer(eTimeout, TimerRef, Timers);
               _ ->
                  Timers
            end,
         if
            IsPostpone ->
               NewCycleData = setelement(#cycleData.postponed, CycleData, [Event | Postponed]),
               performTimeouts(NewCycleData, NewStatus, CurState, NewDebug, LeftEvents, Timeouts, NextEvents, LastTimers, IsHibernate, DoAfter);
            true ->
               performTimeouts(CycleData, NewStatus, CurState, NewDebug, LeftEvents, Timeouts, NextEvents, LastTimers, IsHibernate, DoAfter)
         end;
      true ->
         %% 取消 status and event timeout
         LastTimers =
            case Timers of
               #{sTimeout := {STimerRef, _STimeoutMsg}} ->
                  TemTimer = cancelTimer(sTimeout, STimerRef, Timers),
                  case TemTimer of
                     #{eTimeout := {ETimerRef, _ETimeoutMsg}} ->
                        cancelTimer(eTimeout, ETimerRef, TemTimer);
                     _ ->
                        TemTimer
                  end;
               _ ->
                  case Timers of
                     #{eTimeout := {ETimerRef, _ETimeoutMsg}} ->
                        cancelTimer(eTimeout, ETimerRef, Timers);
                     _ ->
                        Timers
                  end
            end,
         NewCycleData = setelement(#cycleData.postponed, CycleData, []),

         %% 状态发生改变 重试推迟的事件
         if
            IsPostpone ->
               NewLeftEvents =
                  case Postponed of
                     [] ->
                        [Event | LeftEvents];
                     [E1] ->
                        [E1, Event | LeftEvents];
                     [E2, E1] ->
                        [E1, E2, Event | LeftEvents];
                     _ ->
                        lists:reverse(Postponed, [Event | LeftEvents])
                  end,
               performTimeouts(NewCycleData, NewStatus, CurState, NewDebug, NewLeftEvents, Timeouts, NextEvents, LastTimers, IsHibernate, DoAfter);
            true ->
               NewLeftEvents =
                  case Postponed of
                     [] ->
                        LeftEvents;
                     [E1] ->
                        [E1 | LeftEvents];
                     [E2, E1] ->
                        [E1, E2 | LeftEvents];
                     _ ->
                        lists:reverse(Postponed, LeftEvents)
                  end,
               performTimeouts(NewCycleData, NewStatus, CurState, NewDebug, NewLeftEvents, Timeouts, NextEvents, LastTimers, IsHibernate, DoAfter)
         end
   end.

%% 通过超时和插入事件的处理继续状态转换
performTimeouts(#cycleData{timers = OldTimer} = CycleData, CurStatus, CurState, Debug, LeftEvents, Timeouts, NextEvents, CurTimers, IsHibernate, DoAfter) ->
   TemLastEvents =
      case NextEvents of
         [] ->
            LeftEvents;
         [E1] ->
            [E1 | LeftEvents];
         [E2, E1] ->
            [E1, E2 | LeftEvents];
         _ ->
            lists:reverse(NextEvents, LeftEvents)
      end,
   case Timeouts of
      [] ->
         case OldTimer =:= CurTimers of
            true ->
               performEvents(CycleData, CurStatus, CurState, Debug, TemLastEvents, IsHibernate, DoAfter);
            _ ->
               NewCycleData = setelement(#cycleData.timers, CycleData, CurTimers),
               performEvents(NewCycleData, CurStatus, CurState, Debug, TemLastEvents, IsHibernate, DoAfter)
         end;
      _ ->
         %% 下面执行 loopTimeoutList 时 列表顺序跟最开始的是发的
         {NewTimers, TimeoutEvents, NewDebug} = loopTimeoutsList(lists:reverse(Timeouts), CurTimers, [], Debug),
         case TimeoutEvents of
            [] ->
               case OldTimer =:= NewTimers of
                  true ->
                     performEvents(CycleData, CurStatus, CurState, NewDebug, TemLastEvents, IsHibernate, DoAfter);
                  _ ->
                     NewCycleData = setelement(#cycleData.timers, CycleData, NewTimers),
                     performEvents(NewCycleData, CurStatus, CurState, NewDebug, TemLastEvents, IsHibernate, DoAfter)
               end;
            _ ->
               {LastEvents, LastDebug} = mergeTimeoutEvents(TimeoutEvents, CurStatus, NewDebug, TemLastEvents),
               case OldTimer =:= NewTimers of
                  true ->
                     performEvents(CycleData, CurStatus, CurState, LastDebug, LastEvents, IsHibernate, DoAfter);
                  _ ->
                     NewCycleData = setelement(#cycleData.timers, CycleData, NewTimers),
                     performEvents(NewCycleData, CurStatus, CurState, LastDebug, LastEvents, IsHibernate, DoAfter)
               end
         end
   end.

loopTimeoutsList([], Timers, TimeoutEvents, Debug) ->
   {Timers, TimeoutEvents, Debug};
loopTimeoutsList([OneTimeout | LeftTimeouts], Timers, TimeoutEvents, Debug) ->
   case OneTimeout of
      {TimeoutType, Time, TimeoutMsg, Options} ->
         case Time of
            infinity ->
               NewTimers = doCancelTimer(TimeoutType, Timers),
               loopTimeoutsList(LeftTimeouts, NewTimers, TimeoutEvents, Debug);
            0 when Options =:= [] ->
               NewTimers = doCancelTimer(TimeoutType, Timers),
               loopTimeoutsList(LeftTimeouts, NewTimers, [{TimeoutType, TimeoutMsg} | TimeoutEvents], Debug);
            _ ->
               TimerRef = erlang:start_timer(Time, self(), TimeoutType, Options),
               NewDebug = ?SYS_DEBUG(Debug, {start_timer, {TimeoutType, Time, TimeoutMsg, Options}}),
               NewTimers = doRegisterTimer(TimeoutType, TimerRef, TimeoutMsg, Timers),
               loopTimeoutsList(LeftTimeouts, NewTimers, TimeoutEvents, NewDebug)
         end;
      {TimeoutType, Time, TimeoutMsg} ->
         case Time of
            infinity ->
               NewTimers = doCancelTimer(TimeoutType, Timers),
               loopTimeoutsList(LeftTimeouts, NewTimers, TimeoutEvents, Debug);
            0 ->
               NewTimers = doCancelTimer(TimeoutType, Timers),
               loopTimeoutsList(LeftTimeouts, NewTimers, [{TimeoutType, TimeoutMsg} | TimeoutEvents], Debug);
            _ ->
               TimerRef = erlang:start_timer(Time, self(), TimeoutType),
               NewDebug = ?SYS_DEBUG(Debug, {start_timer, {TimeoutType, Time, TimeoutMsg, []}}),
               NewTimers = doRegisterTimer(TimeoutType, TimerRef, TimeoutMsg, Timers),
               loopTimeoutsList(LeftTimeouts, NewTimers, TimeoutEvents, NewDebug)
         end;
      {UpdateTimeoutType, NewTimeoutMsg} ->
         NewTimers = doUpdateTimer(UpdateTimeoutType, NewTimeoutMsg, Timers),
         loopTimeoutsList(LeftTimeouts, NewTimers, TimeoutEvents, Debug);
      CancelTimeoutType ->
         NewTimers = doCancelTimer(CancelTimeoutType, Timers),
         loopTimeoutsList(LeftTimeouts, NewTimers, TimeoutEvents, Debug)
   end.

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

-compile({inline, [cancelTimer/3]}).
cancelTimer(TimeoutType, TimerRef, Timers) ->
   case erlang:cancel_timer(TimerRef) of
      false ->
         %% 找不到计时器，我们还没有看到超时消息
         receive
            {timeout, TimerRef, TimeoutType} ->
               %% 丢弃该超时消息
               ok
         after 0 ->
            ok
         end;
      _ ->
         %% Timer 已经运行了
         ok
   end,
   maps:remove(TimeoutType, Timers).

%% 排队立即超时事件（超时0事件）
%% 自事件超时0起，事件得到特殊处理
%% 任何收到的事件都会取消事件超时，
%% 因此，如果在事件超时0事件之前存在入队事件-事件超时被取消，因此没有事件。
%% 其他（status_timeout和{timeout，Name}）超时0个事件
%% 在事件计时器超时0事件之后发生的事件被认为是
%% 属于在事件计时器之后启动的计时器
%% 已触发超时0事件，因此它们不会取消事件计时器。
mergeTimeoutEvents([], _Status, Debug, Events) ->
   {Events, Debug};
mergeTimeoutEvents([{eTimeout, _} = TimeoutEvent | TimeoutEvents], Status, Debug, []) ->
   %% 由于队列中没有其他事件，因此添加该事件零超时事件
   NewDebug = ?SYS_DEBUG(Debug, {insert_timeout, TimeoutEvent, Status}),
   mergeTimeoutEvents(TimeoutEvents, Status, NewDebug, [TimeoutEvent]);
mergeTimeoutEvents([{eTimeout, _} | TimeoutEvents], Status, Debug, Events) ->
   %% 忽略，因为队列中还有其他事件，因此它们取消了事件超时0。
   mergeTimeoutEvents(TimeoutEvents, Status, Debug, Events);
mergeTimeoutEvents([TimeoutEvent | TimeoutEvents], Status, Debug, Events) ->
   %% Just prepend all others
   NewDebug = ?SYS_DEBUG(Debug, {insert_timeout, TimeoutEvent, Status}),
   mergeTimeoutEvents(TimeoutEvents, Status, NewDebug, [TimeoutEvent | Events]).

%% Return a list of all pending timeouts
list_timeouts(Timers) ->
   {maps:size(Timers),
      maps:fold(
         fun(TimeoutType, {_TimerRef, TimeoutMsg}, Acc) ->
            [{TimeoutType, TimeoutMsg} | Acc]
         end, [], Timers)}.
%%---------------------------------------------------------------------------

%% 状态转换已完成，如果有排队事件，则继续循环，否则获取新事件
performEvents(CycleData, CurStatus, CurState, Debug, LeftEvents, IsHibernate, DoAfter) ->
%  io:format("loop_done: status_data = ~p ~n postponed = ~p  LeftEvents = ~p ~n timers = ~p.~n", [S#status.status_data,,S#status.postponed,LeftEvents,S#status.timers]),
   case DoAfter of
      {true, Args} ->
         %% 这里 IsHibernate设置会被丢弃 按照gen_server中的设计 continue 和 hiernate是互斥的
         startAfterCall(CycleData, CurStatus, CurState, Debug, LeftEvents, Args);
      _ ->
         case LeftEvents of
            [] ->
               reLoopEntry(CycleData, CurStatus, CurState, Debug, IsHibernate);
            [Event | _Events] ->
               %% 循环直到没有排队事件
               if
                  IsHibernate ->
                     %% _ = garbage_collect(),
                     erts_internal:garbage_collect(major);
                  true ->
                     ignore
               end,
               startEventCall(CycleData, CurStatus, CurState, Debug, LeftEvents, Event)
         end
   end.

replyThenTerminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents, Replies) ->
   NewDebug = ?SYS_DEBUG(Debug, {out, Replies}),
   try
      terminate(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, NewDebug, LeftEvents)
   after
      case Replies of
         {reply, From, Reply} ->
            reply(From, Reply);
         _ ->
            [reply(From, Reply) || {reply, From, Reply} <- Replies]
      end
   end.

terminate(Class, Reason, Stacktrace, #cycleData{module = Module} = CycleData, CurStatus, CurState, Debug, LeftEvents) ->
   epmStopAll(),
   case erlang:function_exported(Module, terminate, 3) of
      true ->
         try Module:terminate(Reason, CurStatus, CurState) of
            _ -> ok
         catch
            throw:_ -> ok;
            C:R:ST ->
               error_info(C, R, ST, CycleData, CurStatus, CurState, Debug, LeftEvents),
               erlang:raise(C, R, ST)
         end;
      false ->
         ok
   end,
   case Reason of
      normal ->
         ?SYS_DEBUG(Debug, {terminate, Reason, CurStatus});
      shutdown ->
         ?SYS_DEBUG(Debug, {terminate, Reason, CurStatus});
      {shutdown, _} ->
         ?SYS_DEBUG(Debug, {terminate, Reason, CurStatus});
      _ ->
         error_info(Class, Reason, Stacktrace, CycleData, CurStatus, CurState, Debug, LeftEvents)
   end,
   case Stacktrace of
      [] ->
         erlang:Class(Reason);
      _ ->
         erlang:raise(Class, Reason, Stacktrace)
   end.

error_info(Class, Reason, Stacktrace, #cycleData{isEnter = IsEnter, postponed = Postponed, timers = Timers} = CycleData, CurStatus, CurState, Debug, LeftEvents) ->
   Log = sys:get_log(Debug),
   ?LOG_ERROR(
      #{
         label => {gen_ipc, terminate},
         name => getProName(),
         queue => LeftEvents,
         postponed => Postponed,
         isEnter => IsEnter,
         status => format_status(terminate, get(), CycleData, CurStatus, CurState),
         timeouts => list_timeouts(Timers),
         log => Log,
         reason => {Class, Reason, Stacktrace},
         client_info => clientStacktrace(LeftEvents)
      },
      #{
         domain => [otp],
         report_cb => fun gen_ipc:format_log/1,
         error_logger => #{tag => error}}
   ).

clientStacktrace([]) ->
   undefined;
clientStacktrace([{{call, {Pid, _Tag}}, _Req} | _]) when is_pid(Pid) ->
   if
      node(Pid) =:= node() ->
         case process_info(Pid, [current_stacktrace, registered_name]) of
            undefined ->
               {Pid, dead};
            [{current_stacktrace, Stacktrace}, {registered_name, []}] ->
               {Pid, {Pid, Stacktrace}};
            [{current_stacktrace, Stacktrace}, {registered_name, Name}] ->
               {Pid, {Name, Stacktrace}}
         end;
      true ->
         {Pid, remote}
   end;
clientStacktrace([_ | _]) ->
   undefined.

format_log(#{label:={gen_statusm, terminate},
   name:=Name,
   queue:=LeftEvents,
   postponed:=Postponed,
   callback_mode:=CallbackMode,
   status_enter:=StatusEnter,
   status:=FmtData,
   timeouts:=Timeouts,
   log:=Log,
   reason:={Class, Reason, Stacktrace},
   client_info:=ClientInfo}) ->
   {FixedReason, FixedStacktrace} =
      case Stacktrace of
         [{M, F, Args, _} | ST]
            when Class =:= error, Reason =:= undef ->
            case code:is_loaded(M) of
               false ->
                  {{'module could not be loaded', M}, ST};
               _ ->
                  Arity =
                     if
                        is_list(Args) ->
                           length(Args);
                        is_integer(Args) ->
                           Args
                     end,
                  case erlang:function_exported(M, F, Arity) of
                     true ->
                        {Reason, Stacktrace};
                     false ->
                        {{'function not exported', {M, F, Arity}}, ST}
                  end
            end;
         _ -> {Reason, Stacktrace}
      end,
   {ClientFmt, ClientArgs} = format_client_log(ClientInfo),
   CBMode =
      case StatusEnter of
         true ->
            [CallbackMode, status_enter];
         false ->
            CallbackMode
      end,
   {"** Status machine ~tp terminating~n" ++
      case LeftEvents of
         [] -> "";
         _ -> "** Last event = ~tp~n"
      end ++
      "** When server status  = ~tp~n" ++
      "** Reason for termination = ~w:~tp~n" ++
      "** Callback mode = ~p~n" ++
      case LeftEvents of
         [_, _ | _] -> "** Queued = ~tp~n";
         _ -> ""
      end ++
      case Postponed of
         [] -> "";
         _ -> "** Postponed = ~tp~n"
      end ++
      case FixedStacktrace of
         [] -> "";
         _ -> "** Stacktrace =~n**  ~tp~n"
      end ++
      case Timeouts of
         {0, _} -> "";
         _ -> "** Time-outs: ~p~n"
      end ++
      case Log of
         [] -> "";
         _ -> "** Log =~n**  ~tp~n"
      end ++ ClientFmt,
         [Name |
            case LeftEvents of
               [] -> [];
               [Event | _] -> [error_logger:limit_term(Event)]
            end] ++
         [error_logger:limit_term(FmtData),
            Class, error_logger:limit_term(FixedReason),
            CBMode] ++
         case LeftEvents of
            [_ | [_ | _] = Events] -> [error_logger:limit_term(Events)];
            _ -> []
         end ++
         case Postponed of
            [] -> [];
            _ -> [error_logger:limit_term(Postponed)]
         end ++
         case FixedStacktrace of
            [] -> [];
            _ -> [error_logger:limit_term(FixedStacktrace)]
         end ++
         case Timeouts of
            {0, _} -> [];
            _ -> [error_logger:limit_term(Timeouts)]
         end ++
         case Log of
            [] -> [];
            _ -> [[error_logger:limit_term(T) || T <- Log]]
         end ++ ClientArgs}.

format_client_log(undefined) ->
   {"", []};
format_client_log({Pid, dead}) ->
   {"** Client ~p is dead~n", [Pid]};
format_client_log({Pid, remote}) ->
   {"** Client ~p is remote on node ~p~n", [Pid, node(Pid)]};
format_client_log({_Pid, {Name, Stacktrace}}) ->
   {"** Client ~tp stacktrace~n"
   "** ~tp~n", [Name, error_logger:limit_term(Stacktrace)]}.

%% Call Module:format_status/2 or return a default value
format_status(Opt, PDict, #cycleData{module = Module}, CurStatus, CurState) ->
   case erlang:function_exported(Module, format_status, 2) of
      true ->
         try Module:formatStatus(Opt, [PDict, CurStatus, CurState])
         catch
            throw:Result ->
               Result;
            _:_ ->
               format_status_default(Opt, {CurStatus, atom_to_list(Module) ++ ":format_status/2 crashed"})
         end;
      false ->
         format_status_default(Opt, {CurStatus, CurState})
   end.

%% The default Module:format_status/3
format_status_default(Opt, Status_State) ->
   case Opt of
      terminate ->
         Status_State;
      _ ->
         [{data, [{"Status", Status_State}]}]
   end.

%%---------------------------------------------------------------------------
%% Format debug messages.  Print them as the call-back module sees
%% them, not as the real erlang messages.  Use trace for that.
%%--------------------------------------------------------------------------
print_event(Dev, SystemEvent, Name) ->
   case SystemEvent of
      {in, Event, Status} ->
         io:format(Dev, "*DBG* ~tp receive ~ts in status ~tp~n", [Name, event_string(Event), Status]);
      {code_change, Event, Status} ->
         io:format(Dev, "*DBG* ~tp receive ~ts after code change in status ~tp~n", [Name, event_string(Event), Status]);
      {out, Reply, {To, _Tag}} ->
         io:format(Dev, "*DBG* ~tp send ~tp to ~tw~n", [Name, Reply, To]);
      {out, Replies} ->
         io:format(Dev, "*DBG* ~tp sendto list ~tw~n", [Name, Replies]);
      {enter, Status} ->
         io:format(Dev, "*DBG* ~tp enter in status ~tp~n", [Name, Status]);
      {start_timer, Action, Status} ->
         io:format(Dev, "*DBG* ~tp start_timer ~tp in status ~tp~n", [Name, Action, Status]);
      {insert_timeout, Event, Status} ->
         io:format(Dev, "*DBG* ~tp insert_timeout ~tp in status ~tp~n", [Name, Event, Status]);
      {terminate, Reason, Status} ->
         io:format(Dev, "*DBG* ~tp terminate ~tp in status ~tp~n", [Name, Reason, Status]);
      {Tag, Event, Status, NextStatus}
         when Tag =:= postpone; Tag =:= consume ->
         StatusString =
            case NextStatus of
               Status ->
                  io_lib:format("~tp", [Status]);
               _ ->
                  io_lib:format("~tp => ~tp", [Status, NextStatus])
            end,
         io:format(Dev, "*DBG* ~tp ~tw ~ts in status ~ts~n", [Name, Tag, event_string(Event), StatusString])
   end.

event_string(Event) ->
   case Event of
      {{call, {Pid, _Tag}}, Request} ->
         io_lib:format("call ~tp from ~tw", [Request, Pid]);
      {EventType, EventContent} ->
         io_lib:format("~tw ~tp", [EventType, EventContent])
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%% 进程字典操作函数 %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
setParent(Parent) ->
   erlang:put(?PD_PARENT, Parent).

getParent() ->
   erlang:get(?PD_PARENT).

setProName(Name) ->
   erlang:put(?PD_PRO_NAME, Name).

getProName() ->
   erlang:get(?PD_PRO_NAME).

setEpmList(EpmList) ->
   erlang:put(?PD_EPM_LIST, EpmList).

getEpmList() ->
   case erlang:get(?PD_EPM_LIST) of
      undefined ->
         [];
      RetList ->
         RetList
   end.

delEpmHer(#epmHer{epmId = EpmId}) ->
   erlang:erase({?PD_EPM_FLAG, EpmId}).

setEpmHer(#epmHer{epmId = EpmId} = EpmHer) ->
   erlang:put({?PD_EPM_FLAG, EpmId}, EpmHer).
getEpmHer(EpmId) ->
   erlang:get({?PD_EPM_FLAG, EpmId}).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%