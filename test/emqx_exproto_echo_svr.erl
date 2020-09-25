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

-module(emqx_exproto_echo_svr).

-behavior(emqx_exproto_v_1_connection_handler_bhvr).

-export([ start/0
        , stop/1
        ]).

-export([ on_socket_created/2
        , on_received_bytes/2
        , on_socket_closed/2
        , on_received_messages/2
        ]).

-define(HTTP, #{grpc_opts => #{service_protos => [emqx_exproto_pb],
                               services => #{'emqx.exproto.v1.ConnectionHandler' => ?MODULE}},
                listen_opts => #{port => 9001,
                                 socket_options => []},
                pool_opts => #{size => 8},
                transport_opts => #{ssl => false}}).

-define(CLIENT, emqx_exproto_v_1_connection_adapter_client).
-define(send(Req), ?CLIENT:send(Req, #{channel => ct_test_channel})).
-define(close(Req), ?CLIENT:close(Req, #{channel => ct_test_channel})).
-define(authenticate(Req), ?CLIENT:authenticate(Req, #{channel => ct_test_channel})).
-define(publish(Req), ?CLIENT:publish(Req, #{channel => ct_test_channel})).
-define(subscribe(Req), ?CLIENT:subscribe(Req, #{channel => ct_test_channel})).
-define(unsubscribe(Req), ?CLIENT:unsubscribe(Req, #{channel => ct_test_channel})).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start() ->
    application:ensure_all_started(grpcbox),
    [start_channel(), start_server()].

start_channel() ->
    grpcbox_channel_sup:start_child(ct_test_channel, [{http, "localhost", 9100, []}], #{}).

start_server() ->
    grpcbox:start_server(?HTTP).

stop([ChannPid, SvrPid]) ->
    supervisor:terminate_child(grpcbox_channel_sup, ChannPid),
    supervisor:terminate_child(grpcbox_services_simple_sup, SvrPid).

%%--------------------------------------------------------------------
%% Protocol Adapter callbacks
%%--------------------------------------------------------------------

-spec on_socket_created(ctx:ctx(), emqx_exproto_pb:created_socket_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_socket_created(Ctx, Req) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_received_bytes(ctx:ctx(), emqx_exproto_pb:received_bytes_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_received_bytes(Ctx, Req = #{conn := Conn, bytes := Bytes}) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    ClientInfo = #{proto_name => <<"echo">>,
                   proto_ver => <<"0.1">>,
                   clientid => <<"echo_conn_1">>
                  },
    case ?authenticate(#{conn => Conn, clientinfo => ClientInfo}) of
        {ok, #{result := true}, _} ->
            ?subscribe(#{conn => Conn, topic => <<"t/#">>, qos => 0}),
            ?send(#{conn => Conn, bytes => Bytes}),
            {ok, #{}, Ctx};
        _ ->
            {ok, #{}, Ctx}
    end.

-spec on_socket_closed(ctx:ctx(), emqx_exproto_pb:socket_closed_request()) ->
    {ok, emqx_exproto_pb:empty_success(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_socket_closed(Ctx, Req) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_received_messages(ctx:ctx(), emqx_exproto_pb:received_messages_request()) ->
    {ok, emqx_exproto_pb:bool_result(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_received_messages(Ctx, Req = #{conn := Conn, messages := Messages}) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    lists:foreach(fun(#{topic := Topic, qos := Qos, payload := Payload}) ->
        ?publish(#{conn => Conn, topic => Topic, qos => Qos, payload => Payload})
    end, Messages),
    {ok, #{}, Ctx}.
