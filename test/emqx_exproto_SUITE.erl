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

-module(emqx_exproto_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").

-define(TCPOPTS, [binary, {active, false}]).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [{group, Name} || Name  <- metrics()].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{Name, Cases} || Name <- metrics()].

%% @private
metrics() ->
    [ list_to_atom(X ++ "_" ++ Y)
      || X <- ["python3"], Y <- ["tcp", "ssl", "udp", "dtls"]].

init_per_group(GrpName, Config) ->
    [Lang, LisType] = [list_to_atom(X) || X <- string:tokens(atom_to_list(GrpName), "_")],
    put(grpname, {Lang, LisType}),
    emqx_ct_helpers:start_apps([emqx_exproto], fun set_sepecial_cfg/1),
    [{driver_type, Lang},
     {listener_type, LisType} | Config].

end_per_group(_, _) ->
    emqx_ct_helpers:stop_apps([emqx_exproto]).

set_sepecial_cfg(emqx_exproto) ->
    Path = emqx_ct_helpers:deps_path(emqx_exproto, "example/"),
    Listeners = application:get_env(emqx_exproto, listeners, []),
    Driver = compile(get(grpname), Path),
    NListeners = [{Proto, Type, LisOn, lists:keystore(driver, 1, Opts, {driver, Driver})}
                  || {Proto, Type, LisOn, Opts} <- Listeners],
    application:set_env(emqx_exproto, listeners, NListeners);
set_sepecial_cfg(_App) ->
    ok.

compile({java, _}, Path) ->
    ErlPortJar = emqx_ct_helpers:deps_path(erlport, "priv/java/_pkgs/erlport.jar"),
    ct:pal(os:cmd(lists:concat(["cd ", Path, " && ",
                                "rm -rf Main.class State.class && ",
                                "javac -cp ", ErlPortJar, " Main.java"]))),

    [{type, java}, {path, Path}, {cbm, 'Main'}];
compile({python3, _}, Path) ->
    [{type, python3}, {path, Path}, {cbm, main}].

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_start_stop(_) ->
    ok.

t_echo(_) ->
    {ok, Sock} = gen_tcp:connect("127.0.0.1", 7993, ?TCPOPTS),

    %% tcp echo
    Bin = rand_bytes(),
    gen_tcp:send(Sock, Bin),
    {ok, Bin} = gen_tcp:recv(Sock, byte_size(Bin), 5000),

    %% pubsub echo
    emqx:subscribe(<<"t/#">>),
    emqx:publish(emqx_message:make(<<"t/dn">>, <<"echo">>)),
    First = receive {_, _, X} -> X#message.payload end,
    First = receive {_, _, Y} -> Y#message.payload end,

    gen_tcp:close(Sock).

%%--------------------------------------------------------------------
%% Utils

rand_bytes() ->
    crypto:strong_rand_bytes(rand:uniform(256)).


%%--------------------------------------------------------------------
%% Opts

conn_opts() ->
    [{active_n, 100},
     {idle_timeout, 30000}].

listener_opts() ->
    [{acceptors, 8},
     {max_connections, 1000},
     {max_conn_rate, 100},
     {access_rules, [{allow,all}]}].

tcp_opts() ->
    [{backlog, 100},
     {nodelay, true} | udp_opts()].

udp_opts() ->
    [{send_timeout, 15000},
     {send_timeout_close, true},
     {recbuf, 1024},
     {sndbuf, 1024},
     {buffer, 1024},
     {reuseaddr, true}].

ssl_dtls_opts() ->
    Path = emqx_ct_helpers:deps_path(emqx_exproto, "certs/"),
    [{versions, ['tlsv1.2','tlsv1.1',tlsv1]},
     {ciphers, ciphers()},
     {handshake_timeout, 15000},
     %{dhfile,"{{ platform_etc_dir }}/certs/dh-params.pem"},
     {keyfile, Path ++ "key.pem"},
     {certfile, Path ++ "cert.pem"},
     {cacertfile, Path ++ "cacert.pem"},
     {verify, verify_peer},
     {fail_if_no_peer_cert, true},
     {secure_renegotiate, false},
     {reuse_sessions, true},
     {honor_cipher_order, true}].

ciphers() ->
    proplists:get_value(ciphers, emqx_ct_helpers:client_ssl()).
