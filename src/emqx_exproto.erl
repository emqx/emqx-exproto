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

-module(emqx_exproto).

-compile({no_auto_import, [register/1]}).

-include("emqx_exproto.hrl").

-export([ start_listeners/0
        , stop_listeners/0
        ]).

%% APIs: Connection level
-export([ send/2
        , close/1
        ]).

%% APIs: Protocol/Session level
-export([ register/2
        , publish/2
        , subscribe/3
        ]).

-type(pmessage() :: any()).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(start_listeners() -> ok).
start_listeners() ->
    lists:foreach(fun start_listener/1, application:get_env(?APP, listeners, [])).

-spec(stop_listeners() -> ok).
stop_listeners() ->
    lists:foreach(fun stop_listener/1, application:get_env(?APP, listeners, [])).

%%--------------------------------------------------------------------
%% APIs - Connection level
%%--------------------------------------------------------------------

-spec(send(pid(), binary()) -> ok).
send(Conn, Data) when is_pid(Conn), is_binary(Data) ->
    emqx_exproto_conn:call(Conn, {send, Data}).

-spec(close(pid()) -> ok).
close(Conn) when is_pid(Conn) ->
    emqx_exproto_conn:call(Conn, close).

%%--------------------------------------------------------------------
%% APIs - Protocol/Session level
%%--------------------------------------------------------------------

-spec(register(pid(), list()) -> ok | {error, any()}).
register(Conn, ClientInfo0) ->
    case parse_clientinfo(ClientInfo0) of
        {ok, ClientInfo} ->
            emqx_exproto_conn:call(Conn, {register, ClientInfo});
        {error, Reason} ->
            {error, Reason}
    end.

-spec(publish(pid(), pmessage()) -> ok | {error, any()}).
publish(Conn, PMsg) when is_pid(Conn), is_list(PMsg) ->
    case parse_pmessage(PMsg) of
        {ok, Msg} ->
            emqx_exproto_conn:call(Conn, {publish, Msg});
        {error, Reason} ->
            {error, Reason}
    end.

-spec(subscribe(pid(), binary(), list()) -> ok | {error, any()}).
subscribe(Conn, Topic, SubOpts0)
  when is_pid(Conn),
       is_binary(Topic),
       is_list(SubOpts0) ->
    case parse_subopts(SubOpts0) of
        {ok, SubOpts} ->
            emqx_exproto_conn:call(Conn, {subscribe, Topic, SubOpts});
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_listener({Proto, LisType, ListenOn, Opts}) ->
    Name = name(Proto, LisType),
    {value, {_, DriverOpts}, LisOpts} = lists:keytake(driver, 1, Opts),
    case emqx_exproto_driver_mngr:ensuer_driver(Name, DriverOpts) of
        {ok, DriverPid}->
            case start_listener(LisType, Name, ListenOn, [{driver_pid, DriverPid} |LisOpts]) of
                {ok, _} ->
                    io:format("Start ~s listener on ~s successfully.~n",
                              [Name, format(ListenOn)]);
                {error, Reason} ->
                    io:format(standard_error, "Failed to start ~s listener on ~s - ~0p~n!",
                              [Name, format(ListenOn), Reason]),
                    error(Reason)
            end;
        {error, Reason} ->
            io:format(standard_error, "Failed to start ~s's driver - ~0p~n!",
                      [Name, Reason]),
            error(Reason)
    end.

%% @private
start_listener(LisType, Name, ListenOn, LisOpts)
  when LisType =:= tcp;
       LisType =:= ssl ->
    SockOpts = esockd:parse_opt(LisOpts),
    esockd:open(Name, ListenOn, merge_tcp_default(SockOpts),
                {emqx_exproto_conn, start_link, [LisOpts-- SockOpts]});

start_listener(udp, Name, ListenOn, LisOpts) ->
    SockOpts = esockd:parse_opt(LisOpts),
    esockd:open_udp(Name, ListenOn, merge_udp_default(SockOpts),
                    {emqx_exproto_conn, start_link, [LisOpts-- SockOpts]});

start_listener(dtls, Name, ListenOn, LisOpts) ->
    SockOpts = esockd:parse_opt(LisOpts),
    esockd:open_dtls(Name, ListenOn, merge_udp_default(SockOpts),
                    {emqx_exproto_conn, start_link, [LisOpts-- SockOpts]}).

stop_listener({Proto, LisType, ListenOn, Opts}) ->
    Name = name(Proto, LisType),
    _ = emqx_exproto_driver_mnagr:stop_driver(Name),
    StopRet = stop_listener(LisType, Name, ListenOn, Opts),
    case StopRet of
        ok -> io:format("Stop ~s listener on ~s successfully.~n",
                        [Name, format(ListenOn)]);
        {error, Reason} ->
            io:format(standard_error, "Failed to stop ~s listener on ~s - ~p~n.",
                      [Name, format(ListenOn), Reason])
    end,
    StopRet.

%% @private
stop_listener(_LisType, Name, ListenOn, _Opts) ->
    esockd:close(Name, ListenOn).

%% @private
name(Proto, LisType) ->
    list_to_atom(io_lib:format("~s:~s", [Proto, LisType])).

%% @private
format({Addr, Port}) when is_list(Addr) ->
    io_lib:format("~s:~w", [Addr, Port]).

%% @private
merge_tcp_default(Opts) ->
    case lists:keytake(tcp_options, 1, Opts) of
        {value, {tcp_options, TcpOpts}, Opts1} ->
            [{tcp_options, emqx_misc:merge_opts(?TCP_SOCKOPTS, TcpOpts)} | Opts1];
        false ->
            [{tcp_options, ?TCP_SOCKOPTS} | Opts]
    end.

merge_udp_default(Opts) ->
    case lists:keytake(udp_options, 1, Opts) of
        {value, {udp_options, TcpOpts}, Opts1} ->
            [{udp_options, emqx_misc:merge_opts(?UDP_SOCKOPTS, TcpOpts)} | Opts1];
        false ->
            [{udp_options, ?UDP_SOCKOPTS} | Opts]
    end.

%%--------------------------------------------------------------------
%% Convertor

parse_clientinfo(_Info) ->
    todo.

parse_pmessage(_Msg) ->
    todo.

parse_subopts(_Opts) ->
    todo.
