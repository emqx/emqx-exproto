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

-include("emqx_exproto.hrl").
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

-spec send(emqx_exproto_pb:send_bytes_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
send(Req = #{conn := Conn, bytes := Bytes}, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response(call(Conn, {send, Bytes})), Md}.

-spec close(emqx_exproto_pb:close_socket_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
close(Req = #{conn := Conn}, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response(call(Conn, close)), Md}.

-spec authenticate(emqx_exproto_pb:authenticate_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
authenticate(Req = #{conn := Conn,
                     password := Password,
                     clientinfo := ClientInfo}, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    case validate(clientinfo, ClientInfo) of
        false ->
            {ok, response({error, ?RESP_REQUIRED_PARAMS_MISSED}), Md};
        _ ->
            {ok, response(call(Conn, {auth, ClientInfo, Password})), Md}
    end.

-spec start_timer(emqx_exproto_pb:timer_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
start_timer(Req = #{conn := Conn, type := Type, interval := Interval}, Md)
  when Type =:= 'KEEPALIVE' andalso Interval > 0 ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response(call(Conn, {start_timer, keepalive, Interval})), Md};
start_timer(Req, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response({error, ?RESP_PARAMS_TYPE_ERROR}), Md}.

-spec publish(emqx_exproto_pb:publish_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
publish(Req = #{conn := Conn, topic := Topic, qos := Qos, payload := Payload}, Md)
  when ?IS_QOS(Qos) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response(call(Conn, {publish, Topic, Qos, Payload})), Md};

publish(Req, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response({error, ?RESP_PARAMS_TYPE_ERROR}), Md}.

-spec subscribe(emqx_exproto_pb:subscribe_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
subscribe(Req = #{conn := Conn, topic := Topic, qos := Qos}, Md)
  when ?IS_QOS(Qos) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response(call(Conn, {subscribe, Topic, Qos})), Md};

subscribe(Req, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response({error, ?RESP_PARAMS_TYPE_ERROR}), Md}.

-spec unsubscribe(emqx_exproto_pb:unsubscribe_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_cowboy_h:error_response()}.
unsubscribe(Req = #{conn := Conn, topic := Topic}, Md) ->
    ?LOG(debug, "Recv ~p function with request ~0p", [?FUNCTION_NAME, Req]),
    {ok, response(call(Conn, {unsubscribe, Topic})), Md}.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

to_pid(ConnStr) ->
    list_to_pid(binary_to_list(ConnStr)).

call(ConnStr, Req) ->
    case catch  to_pid(ConnStr) of
        {'EXIT', {badarg, _}} ->
            {error, ?RESP_PARAMS_TYPE_ERROR,
                    <<"The conn type error">>};
        Pid when is_pid(Pid) ->
            case erlang:is_process_alive(Pid) of
                true ->
                    emqx_exproto_conn:call(Pid, Req);
                false ->
                    {error, ?RESP_CONN_PROCESS_NOT_ALIVE,
                            <<"Connection process is not alive">>}
            end
    end.

%%--------------------------------------------------------------------
%% Data types

stringfy(Reason) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Reason]))).

validate(clientinfo, M) ->
    Required = [proto_name, proto_ver, clientid],
    lists:all(fun(K) -> maps:is_key(K, M) end, Required).

response(ok) ->
    #{code => ?RESP_SUCCESS};
response({error, Code, Reason})
  when ?IS_GRPC_RESULT_CODE(Code) ->
    #{code => Code, message => stringfy(Reason)};
response({error, Code})
  when ?IS_GRPC_RESULT_CODE(Code) ->
    #{code => Code};
response(Other) ->
    #{code => ?RESP_UNKNOWN, message => stringfy(Other)}.
