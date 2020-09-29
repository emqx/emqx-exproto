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

-include("emqx_exproto.hrl").

-export([ start_listeners/0
        , stop_listeners/0
        , start_listener/4
        , stop_listener/4
        ]).

-export([ start_servers/0
        , stop_servers/0
        , start_server/1
        , stop_server/1
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec(start_listeners() -> ok).
start_listeners() ->
    lists:foreach(fun start_listener/1, application:get_env(?APP, listeners, [])).

-spec(stop_listeners() -> ok).
stop_listeners() ->
    lists:foreach(fun stop_listener/1, application:get_env(?APP, listeners, [])).

-spec(start_servers() -> ok).
start_servers() ->
    lists:foreach(fun start_server/1, application:get_env(?APP, servers, [])).

-spec(stop_servers() -> ok).
stop_servers() ->
    lists:foreach(fun stop_server/1, application:get_env(?APP, servers, [])).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

start_server({Name, Port, SSLOptions}) ->
    case emqx_exproto_sup:start_grpc_server(Name, Port, SSLOptions) of
        {ok, _} ->
            io:format("Start ~s gRPC server on ~w successfully.~n",
                      [Name, Port]);
        {error, Reason} ->
            io:format(standard_error, "Failed to start ~s gRPC server on ~w - ~0p~n!",
                      [Name, Port, Reason]),
            error({failed_start_server, Reason})
    end.

stop_server({Name, Port, _SSLOptions}) ->
    ok = emqx_exproto_sup:stop_grpc_server(Name),
    io:format("Stop ~s gRPC server on ~w successfully.~n", [Name, Port]).

start_listener({Proto, LisType, ListenOn, Opts}) ->
    Name = name(Proto, LisType),
    {value, {_, CallbackOpts}, LisOpts} = lists:keytake(handler, 1, Opts),
    {Endpoints, ChannelOptions} = handler_opts(CallbackOpts),
    case emqx_exproto_sup:start_grpc_client_channel(Name, Endpoints, ChannelOptions) of
        {ok, _ClientChannelPid}->
            case start_listener(LisType, Name, ListenOn, [{handler, Name} |LisOpts]) of
                {ok, _} ->
                    io:format("Start ~s listener on ~s successfully.~n",
                              [Name, format(ListenOn)]);
                {error, Reason} ->
                    io:format(standard_error, "Failed to start ~s listener on ~s - ~0p~n!",
                              [Name, format(ListenOn), Reason]),
                    error(Reason)
            end;
        {error, Reason} ->
            io:format(standard_error, "Failed to start ~s's protocol handler - ~0p~n!",
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
    StopRet = stop_listener(LisType, Name, ListenOn, Opts),
    case StopRet of
        ok ->
            _ = emqx_exproto_sup:stop_grpc_client_channel(Name),
            io:format("Stop ~s listener on ~s successfully.~n",
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
    list_to_atom(lists:flatten(io_lib:format("~s:~s", [Proto, LisType]))).

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

%% @private
handler_opts(Opts) ->
    Scheme = proplists:get_value(scheme, Opts),
    Host = proplists:get_value(host, Opts),
    Port = proplists:get_value(port, Opts),
    Options = proplists:get_value(options, Opts, []),
    SslOpts = case Scheme of
                   https -> proplists:get_value(ssl_options, Opts, []);
                   _ -> []
               end,
     {[{Scheme, Host, Port, SslOpts}], maps:from_list(Options)}.
