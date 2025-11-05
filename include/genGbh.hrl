-ifndef(genGbh_H).
-define(genGbh_H, true).

-record(gbhOpts, {
	daemon = false :: boolean()				%% 是否是守护进程模式 try catch 捕捉到出错之后 是否关闭进程 还是仅仅打印日志 调用、handleError
}).

-ifdef(debug_gbh).
%% debug 调试相关宏定义
-define(NOT_DEBUG, []).
-define(SYS_DEBUG(Debug, Name, Msg),
	case Debug of
		?NOT_DEBUG ->
			ok;
		_ ->
			sys:handle_debug(Debug, fun print_event/3, Name, Msg)
	end).
-else.
-define(SYS_DEBUG(Debug, Name, Msg), ok).
-endif.


-endif.
