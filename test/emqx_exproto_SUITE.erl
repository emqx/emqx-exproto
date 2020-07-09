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
    Path = emqx_ct_helpers:deps_path(emqx_exproto, "test/scripts"),
    Listeners = application:get_env(emqx_exproto, listeners, []),
    Driver =
        case get(grpname) of
            java ->
                [{type, java}, {path, Path}, {cbm, main}];
            python3 ->
                [{type, python3}, {path, Path}, {cbm, main}]
        end,
    NListeners = [{Proto, Type, LisOn, lists:keystore(driver, 1, Opts, {driver, Driver})}
                  || {Proto, Type, LisOn, Opts} <- Listeners],
    application:set_env(emqx_exproto, listeners, NListeners);
set_sepecial_cfg(_App) ->
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_start(_) ->
    ok.
