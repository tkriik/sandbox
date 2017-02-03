%%% parallel directory traversal, shitton of procs

-module(pwalk).
-export([walk/1]).

%% Includes

-include_lib("kernel/include/file.hrl").

%% Records

-record(wait_state, { paths = [] :: [string()],
                      pids = sets:new() :: sets:set(pid()) }).

%% API

walk(Path) ->
    Self = self(),
    spawn_link(fun() -> walk(Path, Self) end),
    wait(#wait_state{}).

%% Utils

walk(Path, Parent) ->
    Parent ! {start, self()},
    case file:list_dir(Path) of
        {ok, Filenames} ->
            FullPaths = lists:map(with_path(Path), Filenames),
            Fn = fun(Filename) -> spawn_link(fun() -> walk(Filename, Parent) end) end,
            lists:foreach(Fn, FullPaths);
        {error, enotdir} ->
            Parent ! {file, Path};
        {error, Reason} ->
            io:format("error@walk: ~p~n", [Reason])
    end,
    Parent ! {done, self()},
    ok.

wait(#wait_state{ pids = Pids } = WS) ->
    receive
        {start, Pid} ->
            io:format("gather: ~p~n", [Pid]),
            gather(WS#wait_state{ pids = sets:add_element(Pid, Pids) });
        Other ->
            io:format("warning@wait: unexpected message: ~p~n", [Other])
    end.

gather(#wait_state{ paths = Paths,
                    pids = Pids } = WS) ->
    case sets:size(Pids) of
        0 -> Paths;
        _ -> receive
                 {start, Pid} ->
                     io:format("start: ~p~n", [Pid]),
                     gather(WS#wait_state{ pids = sets:add_element(Pid, Pids) });
                 {file, Path} ->
                     io:format("file: ~p~n", [Path]),
                     gather(WS#wait_state{ paths = [Path | Paths] });
                 {done, Pid} ->
                     io:format("done: ~p~n", [Pid]),
                     gather(WS#wait_state{ pids = sets:del_element(Pid, Pids) });
                 Other ->
                     io:format("warning@wait: unexpected message: ~p~n", [Other])
             end
    end.

with_path(Path) ->
    fun(Filename) -> filename:join(Path, Filename) end.
