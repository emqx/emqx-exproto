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

%% The gRPC server for ConnectionAdapter
-module(emqx_exproto_gsvr).

-behavior(emqx_exproto_v_1_connection_adapter_bhvr).

-include_lib("emqx/include/logger.hrl").

-logger_header("[ExProto gServer]").

-define(IS_QOS(X), (X =:= 0 orelse X =:= 1 orelse X =:= 2)).

%% gRPC server callbacks
-export([ send/2
        , close/2
        , authenticate/2
        , start_timer/2
        , publish/2
        , subscribe/2
        , unsubscribe/2
        ]).

%%--------------------------------------------------------------------
%% gRPC ConnectionAdapter service
%%--------------------------------------------------------------------

-spec send(ctx:ctx(), emqx_exproto_pb:send_bytes_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
send(Ctx, Req = #{conn := Conn, bytes := Bytes}) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case call(Conn, {send, Bytes}) of
        ok ->
            {ok, #{result => true}, Ctx};
        {error, Reason} ->
            ?LOG(warning, "~s send falied, butes: ~p, reason: ~0p",
                 [Conn, Bytes, Reason]),
            {ok, #{result => false, reason => stringfy(Reason) }, Ctx}
    end.

-spec close(ctx:ctx(), emqx_exproto_pb:close_socket_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
close(Ctx, Req = #{conn := Conn}) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case call(Conn, close) of
        ok ->
            {ok, #{result => true}, Ctx};
        {error, Reason} ->
            ?LOG(warning, "~p close socket falied, reason: ~0p",
                 [Conn, Reason]),
            {ok, #{result => false, reason => stringfy(Reason)}, Ctx}
    end.

-spec authenticate(ctx:ctx(), emqx_exproto_pb:authenticate_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
authenticate(Ctx, Req = #{conn := Conn,
                          password := Password,
                          clientinfo := ClientInfo}) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case validate(clientinfo, ClientInfo) of
        false ->
            {ok, #{result => false, reason => <<"Internal params">>}, Ctx};
        _ ->
            case call(Conn, {auth, ClientInfo, Password}) of
                ok ->
                    {ok, #{result => true}, Ctx};
                {error, Reason} ->
                    ?LOG(warning, "~p authenticate falied, clientinfo ~p reason: ~0p",
                         [Conn, ClientInfo, Reason]),
                    {ok, #{result => false, reason => stringfy(Reason)}, Ctx}
            end
    end.

-spec start_timer(ctx:ctx(), emqx_exproto_pb:publish_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
start_timer(Ctx, Req = #{conn := Conn, type := Type, interval := Interval})
  when Type =:= 'KEEPALIVE' andalso Interval > 0 ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case call(Conn, {start_timer, keepalive, Interval}) of
        ok ->
            {ok, #{result => true}, Ctx};
        {error, Reason} ->
            ?LOG(warning, "~p start ~p timer in ~w seconds failed, reason: ~0p",
                 [Conn, Type, Interval]),
            {ok, #{result => false, reason => stringfy(Reason)}, Ctx}
    end;
start_timer(_Ctx, Req) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, #{result => false, reason => <<"Invalid params">>}}.

-spec publish(ctx:ctx(), emqx_exproto_pb:publish_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
publish(Ctx, Req = #{conn := Conn, topic := Topic, qos := Qos, payload := Payload})
  when ?IS_QOS(Qos) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case call(Conn, {publish, Topic, Qos, Payload}) of
        ok ->
            {ok, #{result => true}, Ctx};
        {error, Reason} ->
            ?LOG(warning, "~p publish to ~p, qos: ~p, payload: ~p reason: ~0p",
                 [Conn, Topic, Qos, Payload, Reason]),
            {ok, #{result => false, reason => stringfy(Reason)}, Ctx}
    end;
publish(_Ctx, Req) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, #{result => false, reason => <<"Invalid params">>}}.

-spec subscribe(ctx:ctx(), emqx_exproto_pb:subscribe_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
subscribe(Ctx, Req = #{conn := Conn, topic := Topic, qos := Qos})
  when ?IS_QOS(Qos) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case call(Conn, {subscribe, Topic, Qos}) of
        ok ->
            {ok, #{result => true}, Ctx};
        {error, Reason} ->
            ?LOG(warning, "~p subscribe ~p falied, reason: ~0p",
                 [Conn, Topic, Reason]),
            {ok, #{result => false, reason => stringfy(Reason)}, Ctx}
    end;
subscribe(_Ctx, Req) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, #{result => false, reason => <<"Invalid params">>}}.

-spec unsubscribe(ctx:ctx(), emqx_exproto_pb:unsubscribe_request())
  -> {ok, emqx_exproto_pb:bool_result(), ctx:ctx()}
   | grpcbox_stream:grpc_error_response().
unsubscribe(Ctx, Req = #{conn := Conn, topic := Topic}) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case call(Conn, {unsubscribe, Topic}) of
        ok ->
            {ok, #{result => true}, Ctx};
        {error, Reason} ->
            ?LOG(warning, "~p unsubscribe ~p falied, reason: ~0p",
                 [Conn, Topic, Reason]),
            {ok, #{result => false, reason => stringfy(Reason)}, Ctx}
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

to_pid(ConnStr) ->
    list_to_pid(binary_to_list(ConnStr)).

call(ConnStr, Req) ->
    case catch  to_pid(ConnStr) of
        {'EXIT', {badarg, _}} ->
            {error, bad_conn_type};
        Pid when is_pid(Pid) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    emqx_exproto_conn:call(Pid, Req);
                false ->
                    {error, conn_process_dieied}
            end
    end.

%%--------------------------------------------------------------------
%% Data types

stringfy(Reason) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Reason]))).

validate(clientinfo, M) ->
    Required = [proto_name, proto_ver, clientid],
    lists:all(fun(K) -> maps:is_key(K, M) end, Required).
