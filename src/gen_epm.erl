-module(gen_epm).

-type terminateArgs() ::
   term() |
   stop |
   removeEpm |
   {error, term()} |
   {stop, Reason :: term()} |
   {error, {'EXIT', Reason :: term()}}.

-callback init(InitArgs :: term()) ->
   {ok, State :: term()} |
   {ok, State :: term(), hibernate} |
   {error, Reason :: term()}.

-callback handleEvent(Event :: term(), State :: term()) ->
   kpS |
   removeEpm |
   {ok, NewState :: term()} |
   {ok, NewState :: term(), hibernate} |
   {swapEpm, NewState :: term(), Args1 :: term(), NewHandler :: gen_ipc:epmHandler(), Args2 :: term()}.

-callback handleCall(Request :: term(), State :: term()) ->
   {removeEpm, Reply :: term()} |
   {reply, Reply :: term()} |
   {reply, Reply :: term(), NewState :: term()} |
   {reply, Reply :: term(), NewState :: term(), hibernate} |
   {swapEpm, Reply :: term(), NewState :: term(), Args1 :: term(), NewHandler :: gen_ipc:epmHandler(), Args2 :: term()}.

-callback handleInfo(Info :: term(), State :: term()) ->
   kpS |
   removeEpm |
   {ok, NewState :: term()} |
   {ok, NewState :: term(), hibernate} |
   {swapEpm, NewState :: term(), Args1 :: term(), NewHandler :: gen_ipc:epmHandler(), Args2 :: term()}.

-callback terminate(Args :: terminateArgs(), State :: term()) -> term().

-optional_callbacks([handleEvent/2, handleCall/2, handleInfo/2, terminate/2]).