-module(gen_emm).

-compile(inline).
-compile({inline_size, 128}).

-include_lib("kernel/include/logger.hrl").
-include("genGbh.hrl").

-import(maps, [iterator/1, next/1]).
-import(gen_call, [gcall/3, gcall/4, greply/2, try_greply/2]).

-export([
   %% API for gen_emm
   start/0, start/1, start/2, start_link/0, start_link/1, start_link/2
   , start_monitor/0, start_monitor/1, start_monitor/2
   , stop/1, stop/3
   , call/3, call/4
   , send/3

   , send_request/3, send_request/5
   , wait_response/2, receive_response/2, check_response/2
   , wait_response/3, receive_response/3, check_response/3
   , reqids_new/0, reqids_size/1
   , reqids_add/3, reqids_to_list/1

   , info_notify/2, call_notify/2
   , add_epm/3, add_sup_epm/3, del_epm/3
   , swap_epm/3, swap_sup_epm/3
   , which_epm/1

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
   , wakeupFromHib/5

   %% logger callback
   , format_log/1, format_log/2, print_event/3
]).

-define(STACKTRACE(), element(2, erlang:process_info(self(), current_stacktrace))).

-type epmHandler() ::
   atom() |
   {atom(), term()}.

-type terminateArgs() ::
   term() |
   stop |
   removeEpm |
   {error, term()} |
   {stop, Reason :: term()} |
   {error, {'EXIT', Reason :: term()}}.

-export_type([addEpmRet/0, delEpmRet/0]).

-type addEpmRet() ::
   ok |
   term() |
   {'EXIT', term()}.

-type delEpmRet() ::
   ok |
   term() |
   {'EXIT', term()}.

-type serverName() ::
   {'local', atom()} |
   {'global', term()} |
   {'via', atom(), term()}.

-type serverRef() ::
   pid() |
   (LocalName :: atom())|
   {Name :: atom(), Node :: atom()} |
   {'global', term()} |
   {'via', atom(), term()}.

-type debug_flag() ::
   'trace' |
   'log' |
   'statistics' |
   'debug'|
   {'logfile', string()}.

-type startOpt() ::
   {'timeout', timeout()} |
   {'debug', [debug_flag()]} |
   {'spawn_opt', [proc_lib:start_spawn_option()]} |
   {'hibernate_after', timeout()}.

-type startRet() ::
   {'ok', pid()} |
   {'ok', {pid(), reference()}} |
   {'error', term()}.

-type from() :: {To :: pid(), Tag :: term()}.
-type request_id() :: gen:request_id().
-type request_id_collection() :: gen:request_id_collection().
-type response_timeout() :: timeout() | {abs, integer()}.

-record(epmHer, {
   epmId = undefined :: term(),
   epmM :: atom(),
   epmSup = undefined :: 'undefined' | pid(),
   epmS :: term()
}).

-callback init(InitArgs :: term()) ->
   {ok, State :: term()} |
   {ok, State :: term(), hibernate} |
   {error, Reason :: term()}.

-callback handleEvent(Event :: term(), State :: term()) ->
   kpS |
   removeEpm |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), hibernate} |
   {swapEpm, NewState :: term(), Args1 :: term(), NewHandler :: epmHandler(), Args2 :: term()}.

-callback handleCall(Request :: term(), State :: term()) ->
   {removeEpm, Reply :: term()} |
   {reply, Reply :: term()} |
   {reply, Reply :: term(), NewState :: term()} |
   {reply, Reply :: term(), NewState :: term(), hibernate} |
   {swapEpm, Reply :: term(), NewState :: term(), Args1 :: term(), NewHandler :: epmHandler(), Args2 :: term()}.

-callback handleInfo(Info :: term(), State :: term()) ->
   kpS |
   removeEpm |
   {noreply, NewState :: term()} |
   {noreply, NewState :: term(), hibernate} |
   {swapEpm, NewState :: term(), Args1 :: term(), NewHandler :: epmHandler(), Args2 :: term()}.

-callback terminate(Args :: terminateArgs(), State :: term()) ->
   term().

-callback code_change(OldVsn :: (term() | {down, term()}), State :: term(), Extra :: term()) ->
   {ok, NewState :: term()}.

-callback format_status(Opt, StatusData) -> Status when
   Opt :: 'normal' | 'terminate',
   StatusData :: [PDict | State],
   PDict :: [{Key :: term(), Value :: term()}],
   State :: term(),
   Status :: term().

-optional_callbacks([
   handleInfo/2
   , terminate/2
   , code_change/3
   , format_status/2
]).

-spec start() -> startRet().
start() ->
   gen:start(?MODULE, nolink, ?MODULE, [], []).

-spec start(ServerName :: serverName() | [startOpt()]) -> startRet().
start(ServerName) when is_tuple(ServerName) ->
   gen:start(?MODULE, nolink, ServerName, ?MODULE, [], []);
start(Opts) when is_list(Opts) ->
   gen:start(?MODULE, nolink, ?MODULE, [], Opts).

-spec start(ServerName :: serverName(), Opts :: [startOpt()]) -> startRet().
start(ServerName, Opts) ->
   gen:start(?MODULE, nolink, ServerName, ?MODULE, [], Opts).

-spec start_link() -> startRet().
start_link() ->
   gen:start(?MODULE, link, ?MODULE, [], []).

-spec start_link(ServerName :: serverName() | [startOpt()]) -> startRet().
start_link(ServerName) when is_tuple(ServerName) ->
   gen:start(?MODULE, link, ServerName, ?MODULE, [], []);
start_link(Opts) when is_list(Opts) ->
   gen:start(?MODULE, link, ?MODULE, [], Opts).

-spec start_link(ServerName :: serverName(), Opts :: [startOpt()]) -> startRet().
start_link(ServerName, Opts) ->
   gen:start(?MODULE, link, ServerName, ?MODULE, [], Opts).

-spec start_monitor() -> startRet().
start_monitor() ->
   gen:start(?MODULE, monitor, ?MODULE, [], []).

-spec start_monitor(ServerName :: serverName() | [startOpt()]) -> startRet().
start_monitor(ServerName) when is_tuple(ServerName) ->
   gen:start(?MODULE, monitor, ServerName, ?MODULE, [], []);
start_monitor(Opts) when is_list(Opts) ->
   gen:start(?MODULE, monitor, ?MODULE, [], Opts).

-spec start_monitor(ServerName :: serverName(), Opts :: [startOpt()]) -> startRet().
start_monitor(ServerName, Opts) ->
   gen:start(?MODULE, monitor, ServerName, ?MODULE, [], Opts).

-spec stop(ServerRef :: serverRef()) -> 'ok'.
stop(ServerRef) ->
   gen:stop(ServerRef).

-spec stop(ServerRef :: serverRef(), Reason :: term(), Timeout :: timeout()) -> ok.
stop(ServerRef, Reason, Timeout) ->
   gen:stop(ServerRef, Reason, Timeout).

%% -spec init_it(pid(), 'self' | pid(), emgr_name(), module(), [term()], [_]) ->
init_it(Starter, self, ServerRef, Mod, Args, Options) ->
   init_it(Starter, self(), ServerRef, Mod, Args, Options);
init_it(Starter, Parent, ServerRef, _, _, Options) ->
   process_flag(trap_exit, true),
   Name = gen:name(ServerRef),
   Debug = gen:debug_options(Name, Options),
   HibernateAfterTimeout = gen:hibernate_after(Options),
   proc_lib:init_ack(Starter, {ok, self()}),
   receiveIng(Parent, Name, HibernateAfterTimeout, #{}, Debug, false).

-spec add_epm(serverRef(), epmHandler(), term()) -> ok | {error, existed} | {error, term()}.
add_epm(EpmSrv, EpmHandler, Args) ->
   epmRpc(EpmSrv, {'$addEpm', EpmHandler, Args}).

-spec add_sup_epm(serverRef(), epmHandler(), term()) -> ok | {error, existed} | {error, term()}.
add_sup_epm(EpmSrv, EpmHandler, Args) ->
   epmRpc(EpmSrv, {'$addSupEpm', EpmHandler, Args, self()}).

-spec del_epm(serverRef(), epmHandler(), term()) -> ok | {error, module_not_found}.
del_epm(EpmSrv, EpmHandler, Args) ->
   epmRpc(EpmSrv, {'$delEpm', EpmHandler, Args}).

-spec swap_epm(serverRef(), {epmHandler(), term()}, {epmHandler(), term()}) -> ok | {error, existed} | {error, term()}.
swap_epm(EpmSrv, {H1, A1}, {H2, A2}) ->
   epmRpc(EpmSrv, {'$swapEpm', H1, A1, H2, A2}).

-spec swap_sup_epm(serverRef(), {epmHandler(), term()}, {epmHandler(), term()}) -> ok | {error, existed} | {error, term()}.
swap_sup_epm(EpmSrv, {H1, A1}, {H2, A2}) ->
   epmRpc(EpmSrv, {'$swapSupEpm', H1, A1, H2, A2, self()}).

-spec which_epm(serverRef()) -> [epmHandler()].
which_epm(EpmSrv) ->
   epmRpc(EpmSrv, '$which_handlers').

-spec info_notify(serverRef(), term()) -> 'ok'.
info_notify(EpmSrv, Event) ->
   epmRequest(EpmSrv, {'$epm_info', '$infoNotify', Event}).

-spec call_notify(serverRef(), term()) -> 'ok'.
call_notify(EpmSrv, Event) ->
   epmRpc(EpmSrv, {'$syncNotify', Event}).

-spec call(serverRef(), epmHandler(), term()) -> term().
call(EpmSrv, EpmHandler, Query) ->
   epmRpc(EpmSrv, {'$epmCall', EpmHandler, Query}).

-spec call(serverRef(), epmHandler(), term(), timeout()) -> term().
call(EpmSrv, EpmHandler, Query, Timeout) ->
   epmRpc(EpmSrv, {'$epmCall', EpmHandler, Query}, Timeout).

send(EpmSrv, EpmHandler, Msg) ->
   epmRequest(EpmSrv, {'$epm_info', EpmHandler, Msg}).

-spec send_request(EventMgrRef::serverRef, Handler::epmHandler(), Request::term()) ->
   ReqId::request_id().
send_request(M, Handler, Request) ->
   try
      gen:send_request(M, '$epm_call', {'$epmCall', Handler, Request})
   catch
      error:badarg ->
         error(badarg, [M, Handler, Request])
   end.

-spec send_request(EventMgrRef::serverRef,
   Handler::epmHandler(),
   Request::term(),
   Label::term(),
   ReqIdCollection::request_id_collection()) ->
   NewReqIdCollection::request_id_collection().
send_request(M, Handler, Request, Label, ReqIdCol) ->
   try
      gen:send_request(M, '$epm_call', {'$epmCall', Handler, Request}, Label, ReqIdCol)
   catch
      error:badarg ->
         error(badarg, [M, Handler, Request, Label, ReqIdCol])
   end.

-spec wait_response(ReqId, WaitTime) -> Result when
   ReqId :: request_id(),
   WaitTime :: response_timeout(),
   Response :: {reply, Reply::term()}
   | {error, {Reason::term(), serverRef}},
   Result :: Response | 'timeout'.

wait_response(ReqId, WaitTime) ->
   try gen:wait_response(ReqId, WaitTime) of
      {reply, {error, _} = Err} -> Err;
      Return -> Return
   catch
      error:badarg ->
         error(badarg, [ReqId, WaitTime])
   end.

-spec wait_response(ReqIdCollection, WaitTime, Delete) -> Result when
   ReqIdCollection :: request_id_collection(),
   WaitTime :: response_timeout(),
   Delete :: boolean(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef}},
   Result :: {Response,
      Label::term(),
      NewReqIdCollection::request_id_collection()} |
   'no_request' |
   'timeout'.

wait_response(ReqIdCol, WaitTime, Delete) ->
   try gen:wait_response(ReqIdCol, WaitTime, Delete) of
      {{reply, {error, _} = Err}, Label, NewReqIdCol} ->
         {Err, Label, NewReqIdCol};
      Return ->
         Return
   catch
      error:badarg ->
         error(badarg, [ReqIdCol, WaitTime, Delete])
   end.

-spec receive_response(ReqId, Timeout) -> Result when
   ReqId :: request_id(),
   Timeout :: response_timeout(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef}},
   Result :: Response | 'timeout'.

receive_response(ReqId, Timeout) ->
   try gen:receive_response(ReqId, Timeout) of
      {reply, {error, _} = Err} -> Err;
      Return -> Return
   catch
      error:badarg ->
         error(badarg, [ReqId, Timeout])
   end.

-spec receive_response(ReqIdCollection, Timeout, Delete) -> Result when
   ReqIdCollection :: request_id_collection(),
   Timeout :: response_timeout(),
   Delete :: boolean(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef}},
   Result :: {Response,
      Label::term(),
      NewReqIdCollection::request_id_collection()} |
   'no_request' |
   'timeout'.

receive_response(ReqIdCol, Timeout, Delete) ->
   try gen:receive_response(ReqIdCol, Timeout, Delete) of
      {{reply, {error, _} = Err}, Label, NewReqIdCol} ->
         {Err, Label, NewReqIdCol};
      Return ->
         Return
   catch
      error:badarg ->
         error(badarg, [ReqIdCol, Timeout, Delete])
   end.

-spec check_response(Msg, ReqId) -> Result when
   Msg :: term(),
   ReqId :: request_id(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef}},
   Result :: Response | 'no_reply'.

check_response(Msg, ReqId) ->
   try gen:check_response(Msg, ReqId) of
      {reply, {error, _} = Err} -> Err;
      Return -> Return
   catch
      error:badarg ->
         error(badarg, [Msg, ReqId])
   end.

-spec check_response(Msg, ReqIdCollection, Delete) -> Result when
   Msg :: term(),
   ReqIdCollection :: request_id_collection(),
   Delete :: boolean(),
   Response :: {reply, Reply::term()} |
   {error, {Reason::term(), serverRef}},
   Result :: {Response,
      Label::term(),
      NewReqIdCollection::request_id_collection()} |
   'no_request' |
   'no_reply'.

check_response(Msg, ReqIdCol, Delete) ->
   try gen:check_response(Msg, ReqIdCol, Delete) of
      {{reply, {error, _} = Err}, Label, NewReqIdCol} ->
         {Err, Label, NewReqIdCol};
      Return ->
         Return
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

epmRpc(EpmSrv, Cmd) ->
   try gcall(EpmSrv, '$epm_call', Cmd, infinity) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, call, [EpmSrv, Cmd, infinity]}}, ?STACKTRACE())
   end.

epmRpc(EpmSrv, Cmd, Timeout) ->
   try gcall(EpmSrv, '$epm_call', Cmd, Timeout) of
      {ok, Reply} ->
         Reply
   catch Class:Reason ->
      erlang:raise(Class, {Reason, {?MODULE, call, [EpmSrv, Cmd, Timeout]}}, ?STACKTRACE())
   end.

epmRequest({global, Name}, Msg) ->
   try global:send(Name, Msg),
   ok
   catch _:_ -> ok
   end;
epmRequest({via, RegMod, Name}, Msg) ->
   try RegMod:send(Name, Msg),
   ok
   catch _:_ -> ok
   end;
epmRequest(EpmSrv, Cmd) ->
   EpmSrv ! Cmd,
   ok.

loopEntry(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, true) ->
   proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug]);
loopEntry(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, _) ->
   receiveIng(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, false).

wakeupFromHib(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug) ->
   receiveIng(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, true).

receiveIng(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, IsHib) ->
   receive
      {system, From, Req} ->
         sys:handle_system_msg(Req, From, Parent, ?MODULE, Debug, {ServerName, HibernateAfterTimeout, EpmHers, IsHib}, IsHib);
      {'EXIT', Parent, Reason} ->
         terminate_server(Reason, Parent, ServerName, EpmHers);
      {'$epm_call', From, Request} ->
         epmCallMsg(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, From, Request);
      {'$epm_info', CmdOrEmpHandler, Event} ->
         epmInfoMsg(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, CmdOrEmpHandler, Event);
      Msg ->
         handleMsg(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, Msg)
   after HibernateAfterTimeout ->
      proc_lib:hibernate(?MODULE, wakeupFromHib, [Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug])
   end.

epmCallMsg(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, From, Request) ->
   ?SYS_DEBUG(Debug, ServerName, {call, From, Request}),
   case Request of
      '$which_handlers' ->
         reply(From, maps:keys(EpmHers)),
         receiveIng(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, false);
      {'$addEpm', EpmHandler, Args} ->
         {Reply, NewEpmHers, IsHib} = doAddEpm(EpmHers, EpmHandler, Args, undefined),
         reply(From, Reply),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
      {'$addSupEpm', EpmHandler, Args, EpmSup} ->
         {Reply, NewEpmHers, IsHib} = doAddSupEpm(EpmHers, EpmHandler, Args, EpmSup),
         reply(From, Reply),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
      {'$delEpm', EpmHandler, Args} ->
         {Reply, NewEpmHers} = doDelEpm(EpmHers, EpmHandler, Args),
         reply(From, Reply),
         receiveIng(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, false);
      {'$swapEpm', EpmId1, Args1, EpmId2, Args2} ->
         {Reply, NewEpmHers, IsHib} = doSwapEpm(EpmHers, EpmId1, Args1, EpmId2, Args2),
         reply(From, Reply),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
      {'$swapSupEpm', EpmId1, Args1, EpmId2, Args2, SupPid} ->
         {Reply, NewEpmHers, IsHib} = doSwapSupEpm(EpmHers, EpmId1, Args1, EpmId2, Args2, SupPid),
         reply(From, Reply),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
      {'$syncNotify', Event} ->
         {NewEpmHers, IsHib} = doNotify(EpmHers, handleEvent, Event, false),
         reply(From, ok),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
      {'$epmCall', EpmHandler, Query} ->
         case doEpmHandle(EpmHers, EpmHandler, handleCall, Query, From) of
            {NewEpmHers, IsHib} ->
               loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
            NewEpmHers ->
               loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, false)
         end
   end.

epmInfoMsg(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, CmdOrEmpHandler, Event) ->
   ?SYS_DEBUG(Debug, ServerName, {info, CmdOrEmpHandler, Event}),
   case CmdOrEmpHandler of
      '$infoNotify' ->
         {NewEpmHers, IsHib} = doNotify(EpmHers, handleEvent, Event, false),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
      EpmHandler ->
         case doEpmHandle(EpmHers, EpmHandler, handleInfo, Event, false) of
            {NewEpmHers, IsHib} ->
               loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib);
            NewEpmHers ->
               loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, false)
         end
   end.

handleMsg(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, Msg) ->
   ?SYS_DEBUG(Debug, ServerName, {in, Msg}),
   case Msg of
      {'EXIT', From, Reason} ->
         NewEpmHers = epmStopOne(From, EpmHers, Reason),
         receiveIng(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, false);
      {_From, Tag, stop} ->
         try terminate_server(normal, Parent, ServerName, EpmHers)
         after
            reply(Tag, ok)
         end;
      {_From, Tag, get_modules} ->
         reply(Tag, get_modules(EpmHers)),
         receiveIng(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, false);
      {_From, Tag, which_handlers} ->
         reply(Tag, maps:keys(EpmHers)),
         receiveIng(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, false);
      _ ->
         {NewEpmHers, IsHib} = doNotify(EpmHers, handleInfo, Msg, false),
         loopEntry(Parent, ServerName, HibernateAfterTimeout, NewEpmHers, Debug, IsHib)
   end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% EPM inner fun
addNewEpm(InitRet, EpmHers, Module, EpmId, EpmSup) ->
   case InitRet of
      {ok, State} ->
         EpmHer = #epmHer{epmId = EpmId, epmM = Module, epmS = State, epmSup = EpmSup},
         {ok, EpmHers#{EpmId => EpmHer}, false};
      {ok, State, hibernate} ->
         EpmHer = #epmHer{epmId = EpmId, epmM = Module, epmS = State, epmSup = EpmSup},
         {ok, EpmHers#{EpmId => EpmHer}, true};
      Other ->
         {Other, EpmHers, false}
   end.

doAddEpm(EpmHers, EpmId, Args, EpmSup) ->
   case EpmId of
      {Module, _SubId} ->
         case EpmHers of
            #{EpmId := _EpmHer} ->
               {{error, existed}, EpmHers, false};
            _ ->
               try Module:init(Args) of
                  Result ->
                     addNewEpm(Result, EpmHers, Module, EpmId, EpmSup)
               catch
                  throw:Ret ->
                     addNewEpm(Ret, EpmHers, Module, EpmId, EpmSup);
                  C:R:S ->
                     {{error, {C, R, S}}, EpmHers, false}
               end
         end;
      _ ->
         case EpmHers of
            #{EpmId := _EpmHer} ->
               {{error, existed}, EpmHers, false};
            _ ->
               try EpmId:init(Args) of
                  Result ->
                     addNewEpm(Result, EpmHers, EpmId, EpmId, EpmSup)
               catch
                  throw:Ret ->
                     addNewEpm(Ret, EpmHers, EpmId, EpmId, EpmSup);
                  C:R:S ->
                     {{error, {C, R, S}}, EpmHers, false}
               end
         end
   end.

doAddSupEpm(EpmHers, EpmHandler, Args, EpmSup) ->
   case doAddEpm(EpmHers, EpmHandler, Args, EpmSup) of
      {ok, _, _} = Result ->
         link(EpmSup),
         Result;
      Ret ->
         Ret
   end.

doSwapEpm(EpmHers, EpmId1, Args1, EpmMId, Args2) ->
   case EpmHers of
      #{EpmId1 := #epmHer{epmSup = EpmSup} = EpmHer} ->
         State2 = epmTerminate(EpmHer, Args1, swapped, {swapped, EpmMId, EpmSup}),
         NewEpmHers = maps:remove(EpmId1, EpmHers),
         case EpmSup of
            false ->
               doAddEpm(NewEpmHers, EpmMId, {Args2, State2}, undefined);
            _ ->
               doAddSupEpm(NewEpmHers, EpmMId, {Args2, State2}, EpmSup)
         end;
      _ ->
         doAddEpm(EpmHers, EpmMId, {Args2, undefined}, undefined)
   end.

doSwapSupEpm(EpmHers, EpmId1, Args1, EpmMId, Args2, EpmSup) ->
   case EpmHers of
      #{EpmId1 := #epmHer{epmSup = OldEpmSup} = EpmHer} ->
         State2 = epmTerminate(EpmHer, Args1, swapped, {swapped, EpmMId, OldEpmSup}),
         NewEpmHers = maps:remove(EpmId1, EpmHers),
         doAddSupEpm(NewEpmHers, EpmMId, {Args2, State2}, EpmSup);
      _ ->
         doAddSupEpm(EpmHers, EpmMId, {Args2, undefined}, EpmSup)
   end.

doNotify(EpmHers, Func, Event, _Form) ->
   allNotify(iterator(EpmHers), Func, Event, false, EpmHers, false).

allNotify(Iterator, Func, Event, From, TemEpmHers, IsHib) ->
   case next(Iterator) of
      {K, _V, NextIterator} ->
         case doEpmHandle(TemEpmHers, K, Func, Event, From) of
            {NewEpmHers, NewIsHib} ->
               allNotify(NextIterator, Func, Event, From, NewEpmHers, IsHib orelse NewIsHib);
            NewEpmHers ->
               allNotify(NextIterator, Func, Event, From, NewEpmHers, IsHib)
         end;
      _ ->
         {TemEpmHers, IsHib}
   end.

doEpmHandle(EpmHers, EpmId, Func, Event, From) ->
   case EpmHers of
      #{EpmId := #epmHer{epmM = EpmM, epmS = EpmS} = EpmHer} ->
         try EpmM:Func(Event, EpmS) of
            Result ->
               handleEpmCR(Result, EpmHers, EpmId, EpmHer, Event, From)
         catch
            throw:Ret ->
               handleEpmCR(Ret, EpmHers, EpmId, EpmHer, Event, From);
            Class:Reason:Strace ->
               epmTerminate(EpmHer, {error, {Class, Reason, Strace}}, Event, crash),
               maps:remove(EpmId, EpmHers)
         end;
      _ ->
         try_greply(From, {error, bad_module}),
         EpmHers
   end.

doDelEpm(EpmHers, EpmHandler, Args) ->
   case EpmHers of
      #{EpmHandler := EpmHer} ->
         epmTerminate(EpmHer, Args, delete, normal),
         {ok, maps:remove(EpmHandler, EpmHers)};
      _ ->
         {{error, module_not_found}, EpmHers}
   end.

%% handleEpmCallbackRet
handleEpmCR(Result, EpmHers, EpmId, EpmHer, Event, From) ->
   case Result of
      kpS ->
         EpmHers;
      {noreply, NewEpmS} ->
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         EpmHers#{EpmId := MewEpmHer};
      {noreply, NewEpmS, hibernate} ->
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         {EpmHers#{EpmId := MewEpmHer}, true};
      {swapEpm, NewEpmS, Args1, EpmMId, Args2} ->
         #epmHer{epmSup = EpmSup} = MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         State = epmTerminate(MewEpmHer, Args1, swapped, {swapped, EpmMId, EpmSup}),
         TemEpmHers = maps:remove(EpmId, EpmHers),
         {_, NewEpmHers, IsHib} =
            case EpmSup of
               undefined ->
                  doAddEpm(TemEpmHers, EpmMId, {Args2, State}, undefined);
               _ ->
                  doAddSupEpm(TemEpmHers, EpmMId, {Args2, State}, EpmSup)
            end,
         {NewEpmHers, IsHib};
      {swapEpm, Reply, NewEpmS, Args1, EpmMId, Args2} ->
         reply(From, Reply),
         #epmHer{epmSup = EpmSup} = MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         State = epmTerminate(MewEpmHer, Args1, swapped, {swapped, EpmMId, EpmSup}),
         TemEpmHers = maps:remove(EpmId, EpmHers),
         {_, NewEpmHers, IsHib} =
            case EpmSup of
               undefined ->
                  doAddEpm(TemEpmHers, EpmMId, {Args2, State}, undefined);
               _ ->
                  doAddSupEpm(TemEpmHers, EpmMId, {Args2, State}, EpmSup)
            end,
         {NewEpmHers, IsHib};
      removeEpm ->
         epmTerminate(EpmHer, removeEpm, remove, normal),
         maps:remove(EpmId, EpmHers);
      {removeEpm, Reply} ->
         reply(From, Reply),
         epmTerminate(EpmHer, removeEpm, remove, normal),
         maps:remove(EpmId, EpmHers);
      {reply, Reply} ->
         reply(From, Reply),
         EpmHers;
      {reply, Reply, NewEpmS} ->
         reply(From, Reply),
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         EpmHers#{EpmId := MewEpmHer};
      {reply, Reply, NewEpmS, hibernate} ->
         reply(From, Reply),
         MewEpmHer = setelement(#epmHer.epmS, EpmHer, NewEpmS),
         {EpmHers#{EpmId := MewEpmHer}, true};
      Other ->
         epmTerminate(EpmHer, {error, {bad_ret, Other}}, Event, crash),
         maps:remove(EpmId, EpmHers)
   end.

reportTerminate(EpmHer, crash, {error, Why}, LastIn, _) ->
   reportTerminate2(EpmHer, Why, LastIn);
%% How == normal | shutdown | {swapped, NewHandler, NewSupervisor}
reportTerminate(EpmHer, How, _, LastIn, _) ->
   reportTerminate2(EpmHer, How, LastIn).

reportTerminate2(#epmHer{epmSup = EpmSup, epmId = EpmId, epmS = State} = EpmHer, Reason, LastIn) ->
   report_error(EpmHer, Reason, State, LastIn),
   case EpmSup of
      undefined ->
         ok;
      _ ->
         EpmSup ! {gen_event_EXIT, EpmId, Reason},
         ok
   end.

report_error(_EpmHer, normal, _, _) -> ok;
report_error(_EpmHer, shutdown, _, _) -> ok;
report_error(_EpmHer, {swapped, _, _}, _, _) -> ok;
report_error(#epmHer{epmId = EpmId, epmM = EpmM}, Reason, State, LastIn) ->
   ?LOG_ERROR(
      #{
         label => {gen_emm, epm_terminate},
         handler => {EpmId, EpmM},
         name => undefined,
         last_message => LastIn,
         state => format_status(terminate, EpmM, get(), State),
         reason => Reason
      },
      #{
         domain => [otp],
         report_cb => fun gen_emm:format_log/2,
         error_logger => #{tag => error, report_cb => fun gen_emm:format_log/1}
      }).

epmStopAll(EpmHers) ->
   forStopAll(iterator(EpmHers)).

forStopAll(Iterator) ->
   case next(Iterator) of
      {_K, V, NextIterator} ->
         epmTerminate(V, stop, stop, shutdown),
         case element(#epmHer.epmSup, V) of
            undefined ->
               ignore;
            EpmSup ->
               unlink(EpmSup)
         end,
         forStopAll(NextIterator);
      _ ->
         ok
   end.

epmStopOne(ExitEmpSup, EpmHers, Reason) ->
   forStopOne(iterator(EpmHers), ExitEmpSup, Reason, EpmHers).

forStopOne(Iterator, ExitEmpSup, Reason, TemEpmHers) ->
   case next(Iterator) of
      {K, V, NextIterator} ->
         case element(#epmHer.epmSup, V) =:= ExitEmpSup of
            true ->
               epmTerminate(V, {stop, Reason}, {parent_terminated, {ExitEmpSup, Reason}}, shutdown),
               forStopOne(NextIterator, ExitEmpSup, Reason, maps:remove(K, TemEpmHers));
            _ ->
               forStopOne(NextIterator, ExitEmpSup, Reason, TemEpmHers)
         end;
      _ ->
         TemEpmHers
   end.

epmTerminate(#epmHer{epmM = EpmM, epmS = State} = EpmHer, Args, LastIn, Reason) ->
   case erlang:function_exported(EpmM, terminate, 2) of
      true ->
         Res = (catch EpmM:terminate(Args, State)),
         reportTerminate(EpmHer, Reason, Args, LastIn, Res),
         Res;
      _ ->
         reportTerminate(EpmHer, Reason, Args, LastIn, ok),
         ok
   end.

-compile({inline, [reply/2]}).
-spec reply(From :: from(), Reply :: term()) -> ok.
reply(From, Reply) ->
   greply(From, Reply).

terminate_server(Reason, _Parent, _ServerName, EpmHers) ->
   epmStopAll(EpmHers),
   exit(Reason).

%%-----------------------------------------------------------------
%% Callback functions for system messages handling.
%%-----------------------------------------------------------------
system_continue(Parent, Debug, {ServerName, HibernateAfterTimeout, EpmHers, IsHib}) ->
   loopEntry(Parent, ServerName, HibernateAfterTimeout, EpmHers, Debug, IsHib).

-spec system_terminate(_, _, _, _) -> no_return().
system_terminate(Reason, Parent, _Debug, {ServerName, _HibernateAfterTimeout, EpmHers, _IsHib}) ->
   terminate_server(Reason, Parent, ServerName, EpmHers).

%%-----------------------------------------------------------------
%% Module here is sent in the system msg change_code.  It specifies
%% which module should be changed.
%%-----------------------------------------------------------------
system_code_change({ServerName, HibernateAfterTimeout, EpmHers, IsHib}, Module, OldVsn, Extra) ->
   NewEpmHers = forCodeChange(iterator(EpmHers), Module, OldVsn, Extra, EpmHers),
   {ok, {ServerName, HibernateAfterTimeout, NewEpmHers, IsHib}}.

forCodeChange(Iterator, CModule, OldVsn, Extra, TemEpmHers) ->
   case next(Iterator) of
      {K, #epmHer{epmM = Module, epmS = EpmS} = V, NextIterator} when Module =:= CModule ->
         {ok, NewEpmS} = Module:code_change(OldVsn, EpmS, Extra),
         forCodeChange(NextIterator, CModule, OldVsn, Extra, TemEpmHers#{K := V#epmHer{epmS = NewEpmS}});
      {_, _, NextIterator} ->
         forCodeChange(NextIterator, CModule, OldVsn, Extra, TemEpmHers);
      _ ->
         TemEpmHers
   end.

system_get_state({_ServerName, _HibernateAfterTimeout, EpmHers, _Hib}) ->
   {ok, forGetState(iterator(EpmHers), [])}.

forGetState(Iterator, Acc) ->
   case next(Iterator) of
      {_K, #epmHer{epmId = EpmId, epmM = Module, epmS = EpmS}, NextIterator} ->
         forGetState(NextIterator, [{Module, EpmId, EpmS} | Acc]);
      _ ->
         Acc
   end.

system_replace_state(StateFun, {ServerName, HibernateAfterTimeout, EpmHers, IsHib}) ->
   {NewEpmHers, NStates} = forReplaceState(iterator(EpmHers), StateFun, EpmHers, []),
   {ok, NStates, {ServerName, HibernateAfterTimeout, NewEpmHers, IsHib}}.

forReplaceState(Iterator, StateFun, TemEpmHers, NStates) ->
   case next(Iterator) of
      {K, #epmHer{epmId = EpmId, epmM = Module, epmS = EpmS} = V, NextIterator} ->
         NState = {_, _, NewEpmS} = StateFun({Module, EpmId, EpmS}),
         forReplaceState(NextIterator, StateFun, TemEpmHers#{K := V#epmHer{epmS = NewEpmS}}, [NState | NStates]);
      _ ->
         {TemEpmHers, NStates}
   end.

%%-----------------------------------------------------------------
%% Format debug messages.  Print them as the call-back module sees
%% them, not as the real erlang messages.  Use trace for that.
%%-----------------------------------------------------------------
print_event(Dev, Msg, Name) ->
   case Msg of
      {call, From, Request} ->
         io:format(Dev, "*DBG* ~tp got call ~tp from ~tp ~n", [Name, Request, From]);
      {info, CmdOrEmpHandler, Event} ->
         io:format(Dev, "*DBG* ~tp got info ~tp~n", [CmdOrEmpHandler, Event]);
      {in, Msg} ->
         io:format(Dev, "*DBG* ~tp got in ~tp~n", [Name, Msg]);
      _ ->
         io:format(Dev, "*DBG* ~tp : ~tp~n", [Name, Msg])
   end.

%% format_log/1 is the report callback used by Logger handler
%% error_logger only. It is kept for backwards compatibility with
%% legacy error_logger event handlers. This function must always
%% return {Format,Args} compatible with the arguments in this module's
%% calls to error_logger prior to OTP-21.0.
format_log(Report) ->
   Depth = error_logger:get_format_depth(),
   FormatOpts = #{
      chars_limit => unlimited,
      depth => Depth,
      single_line => false,
      encoding => utf8
   },
   format_log_multi(limit_report(Report, Depth), FormatOpts).

limit_report(Report, unlimited) ->
   Report;
limit_report(#{label := {gen_event, terminate},
   last_message := LastIn,
   state := State,
   reason := Reason} = Report,
   Depth) ->
   Report#{
      last_message => io_lib:limit_term(LastIn, Depth),
      state => io_lib:limit_term(State, Depth),
      reason => io_lib:limit_term(Reason, Depth)
   };
limit_report(#{label := {gen_event, no_handle_info}, message := Msg} = Report, Depth) ->
   Report#{message => io_lib:limit_term(Msg, Depth)}.

%% format_log/2 is the report callback for any Logger handler, except
%% error_logger.
format_log(Report, FormatOpts0) ->
   Default = #{
      chars_limit => unlimited,
      depth => unlimited,
      single_line => false,
      encoding => utf8
   },
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

format_log_single(#{label := {gen_event, terminate},
   handler := Handler,
   name := SName,
   last_message := LastIn,
   state := State,
   reason := Reason},
   #{single_line := true, depth := Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Reason1 = fix_reason(Reason),
   Format1 = lists:append(["Generic event handler ", P, " crashed. "
   "Installed: ", P, ". Last event: ", P,
      ". State: ", P, ". Reason: ", P, "."]),
   Args1 =
      case Depth of
         unlimited ->
            [Handler, SName, Reason1, LastIn, State];
         _ ->
            [Handler, Depth, SName, Depth, Reason1, Depth,
               LastIn, Depth, State, Depth]
      end,
   {Format1, Args1};
format_log_single(#{label := {gen_event, no_handle_info},
   module := Mod,
   message := Msg},
   #{single_line := true, depth := Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Format = lists:append(["Undefined handle_info in ", P, ". Unhandled message: ", P, "."]),
   Args =
      case Depth of
         unlimited ->
            [Mod, Msg];
         _ ->
            [Mod, Depth, Msg, Depth]
      end,
   {Format, Args};
format_log_single(Report, FormatOpts) ->
   format_log_multi(Report, FormatOpts).

format_log_multi(#{label := {gen_event, terminate},
   handler := Handler,
   name := SName,
   last_message := LastIn,
   state := State,
   reason := Reason},
   #{depth := Depth} = FormatOpts) ->
   Reason1 = fix_reason(Reason),
   P = p(FormatOpts),
   Format =
      lists:append(["** gen_event handler ", P, " crashed.\n",
         "** Was installed in ", P, "\n",
         "** Last event was: ", P, "\n",
         "** When handler state == ", P, "\n",
         "** Reason == ", P, "\n"]),
   Args =
      case Depth of
         unlimited ->
            [Handler, SName, LastIn, State, Reason1];
         _ ->
            [Handler, Depth, SName, Depth, LastIn, Depth, State, Depth, Reason1, Depth]
      end,
   {Format, Args};
format_log_multi(#{label := {gen_event, no_handle_info},
   module := Mod,
   message := Msg},
   #{depth := Depth} = FormatOpts) ->
   P = p(FormatOpts),
   Format =
      "** Undefined handle_info in ~p\n"
      "** Unhandled message: " ++ P ++ "\n",
   Args =
      case Depth of
         unlimited ->
            [Mod, Msg];
         _ ->
            [Mod, Msg, Depth]
      end,
   {Format, Args}.

fix_reason({'EXIT', {undef, [{M, F, A, _L} | _] = MFAs} = Reason}) ->
   case code:is_loaded(M) of
      false ->
         {'module could not be loaded', MFAs};
      _ ->
         case erlang:function_exported(M, F, length(A)) of
            true ->
               Reason;
            _ ->
               {'function not exported', MFAs}
         end
   end;
fix_reason({'EXIT', Reason}) ->
   Reason;
fix_reason(Reason) ->
   Reason.

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

%% Message from the release_handler.
%% The list of modules got to be a set, i.e. no duplicate elements!
get_modules(EpmHers) ->
   allMods(iterator(EpmHers), []).

allMods(Iterator, Acc) ->
   case next(Iterator) of
      {_K, V, NextIterator} ->
         allMods(NextIterator, [element(#epmHer.epmM, V) | Acc]);
      _ ->
         lists:usort(Acc)
   end.

%%-----------------------------------------------------------------
%% Status information
%%-----------------------------------------------------------------
format_status(Opt, StatusData) ->
   [PDict, SysState, Parent, _Debug, {ServerName, _HibernateAfterTimeout, EpmHers, _IsHib}] = StatusData,
   Header = gen:format_status_header("Status for gen_emm handler", ServerName),
   FmtMSL = allStateStatus(iterator(EpmHers), Opt, PDict, []),
   [{header, Header}, {data, [{"Status", SysState}, {"Parent", Parent}]}, {items, {"Installed handlers", FmtMSL}}].

allStateStatus(Iterator, Opt, PDict, EpmHers) ->
   case next(Iterator) of
      {_K, #epmHer{epmM = Module, epmS = EpmS} = V, NextIterator} ->
         NewEpmS = format_status(Opt, Module, PDict, EpmS),
         allStateStatus(NextIterator, Opt, PDict, [V#epmHer{epmS = NewEpmS} | EpmHers]);
      _ ->
         EpmHers
   end.

format_status(Opt, Mod, PDict, State) ->
   case erlang:function_exported(Mod, format_status, 2) of
      true ->
         Args = [PDict, State],
         case catch Mod:format_status(Opt, Args) of
            {'EXIT', _} -> State;
            Else -> Else
         end;
      _ ->
         State
   end.

