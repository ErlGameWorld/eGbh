-module(gen_call).

-export([gcall/3, gcall/4, greply/2]).

-define(default_timeout, 5000).

%% We trust the arguments to be correct, i.e
%% Process is either a local or remote pid,
%% or a {Name, Node} tuple (of atoms) and in this
%% case this node (node()) _is_ distributed and Node =/= node().
-define(get_node(Process), case Process of {_S, N} -> N; _ -> node(Process) end).

gcall(Process, Label, Request) ->
   gcall(Process, Label, Request, ?default_timeout).

gcall(Process, Label, Request, Timeout) ->
   %%-----------------------------------------------------------------
   %% Map different specifications of a process to either Pid or
   %% {Name,Node}. Execute the given Fun with the process as only
   %% argument.
   %% -----------------------------------------------------------------
   case where(Process) of
      undefined ->
         exit(noproc);
      PidOrNameNode ->
         %Node = ?get_node(PidOrNameNode),
         try do_call(PidOrNameNode, Label, Request, Timeout)
         catch
            exit:{nodedown, _Node} ->
               %% A nodedown not yet detected by global, pretend that it was.
               exit(noproc)
         end
   end.

-dialyzer({no_improper_lists, do_call/4}).
do_call(Process, Label, Request, Timeout) ->
   CurNode = node(),
   case ?get_node(Process) of
      CurNode ->
         Mref = erlang:monitor(process, Process),
         %% Local without timeout; no need to use alias since we unconditionally
         %% will wait for either a reply or a down message which corresponds to
         %% the process being terminated (as opposed to 'noconnection')...
         case self() of
            Process ->
               exit(calling_self);
            _ ->
               Process ! {Label, {self(), Mref}, Request}
         end,
         receive
            {Mref, Reply} ->
               erlang:demonitor(Mref, [flush]),
               {ok, Reply};
            {'DOWN', Mref, _, _, Reason} ->
               exit(Reason)
         after Timeout ->
            erlang:demonitor(Mref, [flush]),
            receive
               {[alias | Mref], Reply} ->
                  {ok, Reply}
            after 0 ->
               exit(timeout)
            end
         end;
      _PNode ->
         Mref = erlang:monitor(process, Process, [{alias, demonitor}]),
         Tag = [alias | Mref],

         %% OTP-24:
         %% Using alias to prevent responses after 'noconnection' and timeouts.
         %% We however still may call nodes responding via process identifier, so
         %% we still use 'noconnect' on send in order to try to send on the
         %% monitored connection, and not trigger a new auto-connect.
         %%
         erlang:send(Process, {Label, {self(), Tag}, Request}, [noconnect]),

         receive
            {[alias | Mref], Reply} ->
               erlang:demonitor(Mref, [flush]),
               {ok, Reply};
            {'DOWN', Mref, _, _, noconnection} ->
               exit({nodedown, _PNode});
            {'DOWN', Mref, _, _, Reason} ->
               exit(Reason)
         after Timeout ->
            erlang:demonitor(Mref, [flush]),
            receive
               {[alias | Mref], Reply} ->
                  {ok, Reply}
            after 0 ->
               exit(timeout)
            end
         end
   end.

where({global, Name}) -> global:whereis_name(Name);
where({local, Name}) -> whereis(Name);
where({via, Module, Name}) -> Module:whereis_name(Name);
where({Name, Node} = Process) ->
   CurNode = node(),
   case CurNode of
      Node ->
         whereis(Name);
      nonode@nohost ->
         exit({nodedown, Node});
      _ when is_atom(Name), is_atom(Node) ->
         Process;
      _ ->
         undefined
   end;
where(Name) ->
   if
      is_pid(Name) ->
         Name;
      is_atom(Name) ->
         whereis(Name);
      true ->
         undefined
   end.

%%
%% Send a reply to the client.
%%
greply({_To, [alias | Alias] = Tag}, Reply) when is_reference(Alias) ->
   Alias ! {Tag, Reply}, ok;
greply({_To, [[alias | Alias] | _] = Tag}, Reply) when is_reference(Alias) ->
   Alias ! {Tag, Reply}, ok;
greply({To, Tag}, Reply) ->
   try To ! {Tag, Reply}, ok catch _:_ -> ok end.