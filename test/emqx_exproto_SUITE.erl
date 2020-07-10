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
    [{group, java},
     {group, python3}].

groups() ->
    [{java, emqx_ct:all(?MODULE)},
     {python3, emqx_ct:all(?MODULE)}].

init_per_group(GrpName, Config) ->
    put(grpname, GrpName),
    emqx_ct_helpers:start_apps([emqx_exproto], fun set_sepecial_cfg/1),
    Config.

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

compile(java, Path) ->
    ErlPortJar = emqx_ct_helpers:deps_path(erlport, "priv/java/_pkgs/erlport.jar"),
    ct:pal(os:cmd(lists:concat(["cd ", Path, " && ",
                                "rm -rf Main.class State.class && ",
                                "javac -cp ", ErlPortJar, " Main.java"]))),

    [{type, java}, {path, Path}, {cbm, 'Main'}];
compile(python3, Path) ->
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

