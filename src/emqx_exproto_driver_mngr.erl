%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_exproto_driver_mngr).

-behaviour(gen_server).

-include_lib("emqx/include/logger.hrl").

-log_header("[ExProto DMngr]").

-compile({no_auto_import, [erase/1]}).

%% API
-export([start_link/0]).

%% Manager APIs
-export([ ensure_driver/2
        , stop_drivers/0
        , stop_driver/1
        ]).

%% Driver APIs
-export([ lookup/1
        , call/2
        ]).

%% gen_server callbacks
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-define(SERVER, ?MODULE).

-type state() :: #{drivers := [{atom(), pid()}]}.

-type driver() :: #{name := driver_name(),
                    module := driver_module(),
                    pid := pid(),
                    opts := list()
                   }.

-type driver_name() :: atom().

-type driver_module() :: python | java.

-type mfargs() :: {module(), function(), list()}.

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% APIs - Managers
%%--------------------------------------------------------------------

-spec(ensure_driver(driver_name(), list()) -> {ok, pid()} | {error, any()}).
ensure_driver(Name, Opts) ->
    {value, {_, Type}, NOpts} = lists:keytake(type, 1, Opts),
    gen_server:call(?SERVER, {ensure, {Type, Name, Opts}}).

-spec(stop_drivers() -> ok).
stop_drivers() ->
    gen_server:call(?SERVER, stop_all).

-spec(stop_driver(driver_name()) -> ok).
stop_driver(Name) ->
    gen_server:call(?SERVER, {stop, Name}).

%%--------------------------------------------------------------------
%% APIs - Drivers
%%--------------------------------------------------------------------

-spec(lookup(driver_name()) -> {ok, pid()} | {error, any()}).
lookup(Name) ->
    case catch persistent_term:get({?MODULE, Name}) of
        {'EXIT', {badarg, _}} -> {error, not_found};
        Driver when is_map(Driver) -> {ok, Driver}
    end.

-spec(call(driver(), mfargs()) -> {ok, any()} | {error, any()}).
call(_Driver = #{module := Mod, pid := Pid}, MFArgs) ->
    do_call(Mod, DriverPid, MFArgs).

%% @private
do_call(Mod, Pid, {M, F, Args}) ->
    case catch apply(Mod, call, [Pid, M, F, Args]) of
        ok -> ok;
        undefined -> ok;
        {_Ok = 0, Return} -> {ok, Return};
        {_Err = 1, Reason} -> {error, Reason};
        {'EXIT', Reason, Stk} ->
            ?LOG(error, "CALL ~p ~p:~p(~p), exception: ~p, stacktrace ~0p",
                        [Mod, M, F, Args, Reason, Stk]),
            {error, Reason};
        _X ->
            ?LOG(error, "CALL ~p ~p:~p(~p), unknown return: ~0p",
                        [Mod, M, F, Args, _X]),
            {error, unknown_return_format}
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------
init([]) ->
    {ok, #{drivers => []}}.

handle_call({ensure, {Type, Name, Opts}}, _From, State = #{drivers := Drivers}) ->
    case lists:keyfind(Name, 1, Drivers) of
        false ->
            case do_start_driver(Type, Name, Opts) of
                {ok, Pid} ->
                    Driver = #{name => Name,
                               module => module(Type),
                               pid => Pid,
                               opts => Opts},
                    ok = save(Name, Driver),
                    reply({ok, Driver}, State#{drivers => [{Name, Driver} | Drivers]};
                {error, Reason} ->
                    reply({error, Reason}, State)
            end;
        {_, Driver} ->
            reply({ok, Driver}, State)
    end;

handle_call(stop_all, _From, State = #{drivers := Drivers}) ->
    lists:foreach(
      fun({Name, Pid}) ->
        _ = do_stop_drviver(Pid),
        ok = erase(Name)
      end, Drivers),
    reply(ok, State{drivers => []});

handle_call({stop, Name}, _From, State = #{drivers := Drivers}) ->
    case lists:keyfind(Name, 1, Drivers) of
        false ->
            reply({error, not_found}, State);
        {_, Pid} ->
            _ = do_stop_drviver(DriverPid),
            ok = erase(Name),
            reply(ok, State#{drivers => Drivers -- [{Name, Pid}]})
    end;

handle_call(Req, _From, State) ->
    ?WARNING("Unexpected request: ~p", [Req]),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    ?WARNING("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?WARNING("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Is there need stop all drivers?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_start_driver(Type, Name, Opts)
  when Type =:= python2;
       Type =:= python3 ->
    NOpts = resovle_search_path(python, Opts),
    python:start_link([{python, atom_to_list(Type)} | NOpts]);

do_start_driver(Type, Name, Opts)
  when Type =:= java ->
    NOpts = resovle_search_path(java, Opts),
    python:start_link([{java, atom_to_list(Type)} | NOpts]);

do_start_driver(Type, _, _) ->
    {error, {invalid_driver_type, Type}}.

do_stop_drviver(DriverPid) ->
    erlport:stop(DriverPid).
%% @private
resovle_search_path(java, Opts) ->
    case proplists:get_value(java_path, Opts) of
        undefined -> Opts;
        Path ->
            Solved = lists:flatten(
                       lists:join(pathsep(),
                                  [expand_jar_packages(filename:absname(P))
                                   || P <- re:split(Path, pathsep(), [{return, list}]), P /= ""])),
            lists:keystore(java_path, 1, Opts, {java_path, Solved})
    end;
resovle_search_path(_, Opts) ->
    Opts.

%% @private
expand_jar_packages(Path) ->
    IsJarPkgs = fun(Name) ->
                    Ext = filename:extension(Name),
                    Ext == ".jar" orelse Ext == ".zip"
                end,
    case file:list_dir(Path) of
        {ok, []} -> [Path];
        {error, _} -> [Path];
        {ok, Names} ->
            lists:join(pathsep(),
                       [Path] ++ [filename:join([Path, Name]) || Name <- Names, IsJarPkgs(Name)])
    end.

%% @private
pathsep() ->
    case os:type() of
        {win32, _} ->
            ";";
        _ ->
            ":"
    end.

%%--------------------------------------------------------------------
%% Utils

module(python2) -> python;
module(python3) -> python;
module(java) -> java.

call(Req) ->
    gen_server:call(?SERVER, Req).

reply(Term, State) ->
    {reply, Term, State}.

save(Name, {Type, Pid}) ->
    persistent_term:put({?MODULE, Name}, {Type, Pid}).

erase(Name) ->
    persistent_term:erase({?MODULE, Name}).

