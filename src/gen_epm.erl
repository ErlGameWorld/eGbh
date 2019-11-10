-module(gen_epm).

-callback init(InitArgs :: term()) ->
    {ok, State :: term()} |
    {ok, State :: term(), hibernate} |
    {error, Reason :: term()}.

-callback handleEvent(Event :: term(), State :: term()) ->
    ok |
    {ok, NewState :: term()} |
    {ok, NewState :: term(), hibernate} |
    {swapEpm, Args1 :: term(), NewState :: term(), Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    removeEpm.

-callback handleCall(Request :: term(), State :: term()) ->
    ok |
    {reply, Reply :: term()} |
    {reply, Reply :: term(), NewState :: term()} |
    {reply, Reply :: term(), NewState :: term(), hibernate} |
    {swapEpm, Reply :: term(), Args1 :: term(), NewState :: term(), Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    {removeEpm, Reply :: term()}.

-callback handleInfo(Info :: term(), State :: term()) ->
    ok |
    {ok, NewState :: term()} |
    {ok, NewState :: term(), hibernate} |
    {swapEpm, Args1 :: term(), NewState :: term(), Handler2 :: (atom() | {atom(), Id :: term()}), Args2 :: term()} |
    removeEpm.

-callback terminate(Args :: (term() | {stop, Reason :: term()} |
    stop |
    removeEpm |
    {error, {'EXIT', Reason :: term()}} |
    {error, term()}), State :: term()) ->
    term().

-optional_callbacks([handleEvent/2, handleCall/2, handleInfo/2, terminate/2]).