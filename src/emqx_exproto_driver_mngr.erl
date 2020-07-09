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
-define(DEFAULT_CBM, main).

%-type state() :: #{drivers := [{atom(), pid()}]}.

-type driver() :: #{name := driver_name(),
                    module := driver_module(),
                    cbm := module(),
                    pid := pid(),
                    opts := list()
                   }.

-type driver_name() :: atom().

-type driver_module() :: python | java.

-type fargs() :: {atom(), list()}.

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
    {value, {_, Type}, Opts1} = lists:keytake(type, 1, Opts),
    {value, {_, Cbm},  Opts2} = lists:keytake(cbm, 1, Opts1),
    gen_server:call(?SERVER, {ensure, {Type, Name, Cbm, Opts2}}).

-spec(stop_drivers() -> ok).
stop_drivers() ->
    gen_server:call(?SERVER, stop_all).

-spec(stop_driver(driver_name()) -> ok).
stop_driver(Name) ->
    gen_server:call(?SERVER, {stop, Name}).

%%--------------------------------------------------------------------
%% APIs - Drivers
%%--------------------------------------------------------------------

-spec(lookup(driver_name()) -> {ok, driver()} | {error, any()}).
lookup(Name) ->
    case catch persistent_term:get({?MODULE, Name}) of
        {'EXIT', {badarg, _}} -> {error, not_found};
        Driver when is_map(Driver) -> {ok, Driver}
    end.

-spec(call(driver(), fargs()) -> {ok, any()} | {error, any()}).
call(_Driver = #{module := Mod, pid := Pid, cbm := Cbm}, FArgs) ->
    do_call(Mod, Pid, Cbm, FArgs).

%% @private
do_call(Mod, Pid, Cbm, {F, Args}) ->
    case catch apply(Mod, call, [Pid, Cbm, F, Args]) of
        ok -> ok;
        undefined -> ok;
        {_Ok = 0, Return} -> {ok, Return};
        {_Err = 1, Reason} -> {error, Reason};
        {'EXIT', Reason, Stk} ->
            ?LOG(error, "CALL ~p ~p:~p(~p), exception: ~p, stacktrace ~0p",
                        [Mod, Cbm, F, Args, Reason, Stk]),
            {error, Reason};
        _X ->
            ?LOG(error, "CALL ~p ~p:~p(~p), unknown return: ~0p",
                        [Mod, Cbm, F, Args, _X]),
            {error, unknown_return_format}
    end.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, #{drivers => []}}.

handle_call({ensure, {Type, Name, Cbm, Opts}}, _From, State = #{drivers := Drivers}) ->
    case lists:keyfind(Name, 1, Drivers) of
        false ->
            case do_start_driver(Type, Opts) of
                {ok, Pid} ->
                    Driver = #{name => Name,
                               module => module(Type),
                               cbm => Cbm,
                               pid => Pid,
                               opts => Opts},
                    ok = save(Name, Driver),
                    reply({ok, Driver}, State#{drivers => [{Name, Driver} | Drivers]});
                {error, Reason} ->
                    reply({error, Reason}, State)
            end;
        {_, Driver} ->
            reply({ok, Driver}, State)
    end;

handle_call(stop_all, _From, State = #{drivers := Drivers}) ->
    lists:foreach(
      fun({Name, #{pid := Pid}}) ->
        _ = do_stop_drviver(Pid),
        _ = erase(Name)
      end, Drivers),
    reply(ok, State#{drivers => []});

handle_call({stop, Name}, _From, State = #{drivers := Drivers}) ->
    case lists:keyfind(Name, 1, Drivers) of
        false ->
            reply({error, not_found}, State);
        {_, #{pid := Pid}} ->
            _ = do_stop_drviver(Pid),
            _ = erase(Name),
            reply(ok, State#{drivers => Drivers -- [{Name, Pid}]})
    end;

handle_call(Req, _From, State) ->
    ?WARN("Unexpected request: ~p", [Req]),
    {reply, ok, State}.

handle_cast(Msg, State) ->
    ?WARN("Unexpected cast: ~p", [Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    ?WARN("Unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    %% Is there need stop all drivers?
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

do_start_driver(Type, Opts)
  when Type =:= python2;
       Type =:= python3 ->
    NOpts = resovle_search_path(python, Opts),
    python:start_link([{python, atom_to_list(Type)} | NOpts]);

do_start_driver(Type, Opts)
  when Type =:= java ->
    NOpts = resovle_search_path(java, Opts),
    java:start_link([{java, atom_to_list(Type)} | NOpts]);

do_start_driver(Type, _) ->
    {error, {invalid_driver_type, Type}}.

do_stop_drviver(DriverPid) ->
    erlport:stop(DriverPid).
%% @private
resovle_search_path(java, Opts) ->
    case lists:keytake(path, 1, Opts) of
        false -> Opts;
        {value, {_, Path}, NOpts} ->
            Solved = lists:flatten(
                       lists:join(pathsep(),
                                  [expand_jar_packages(filename:absname(P))
                                   || P <- re:split(Path, pathsep(), [{return, list}]), P /= ""])),
            [{java_path, Solved} | NOpts]
    end;
resovle_search_path(python, Opts) ->
    case lists:keytake(path, 1, Opts) of
        false -> Opts;
        {value, {_, Path}, NOpts} ->
            [{python_path, Path} | NOpts]
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

reply(Term, State) ->
    {reply, Term, State}.

save(Name, Driver) ->
    persistent_term:put({?MODULE, Name}, Driver).

erase(Name) ->
    persistent_term:erase({?MODULE, Name}).

