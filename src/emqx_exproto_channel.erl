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

-module(emqx_exproto_channel).

-logger_header("[ExProto Channel]").

-export([ info/1
        , info/2
        , stats/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_deliver/2
        , handle_timeout/3
        , handle_call/2
        , handle_info/2
        , terminate/2
        ]).

-import(emqx_misc,
        [ run_fold/3
        , pipeline/3
        , maybe_apply/2
        ]).

-export_type([channel/0]).

-record(channel, {
          %% Driver
          driver :: emqx_exproto_driver_mnger:driver(),
          %% Socket connInfo
          conninfo :: emqx_types:conninfo(),
          %% Driver level state
          state :: any(),
         }).

-opaque(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec(info(channel()) -> emqx_types:infos()).
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec(info(list(atom())|atom(), channel()) -> term()).
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(zone, #channel{clientinfo = #{zone := Zone}}) ->
    Zone;
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    maybe_apply(fun emqx_session:info/1, Session);
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(keepalive, #channel{keepalive = Keepalive}) ->
    maybe_apply(fun emqx_keepalive:info/1, Keepalive);
info(will_msg, #channel{will_msg = undefined}) ->
    undefined;
info(will_msg, #channel{will_msg = WillMsg}) ->
    emqx_message:to_map(WillMsg);
info(topic_aliases, #channel{topic_aliases = Aliases}) ->
    Aliases;
info(alias_maximum, #channel{alias_maximum = Limits}) ->
    Limits;
info(timers, #channel{timers = Timers}) -> Timers.

%% TODO: Add more stats.
-spec(stats(channel()) -> emqx_types:stats()).
stats(#channel{session = Session})->
    emqx_session:stats(Session).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec(init(emqx_types:conninfo(), proplists:proplist()) -> channel()).
init(ConnInfo = #{peername := {PeerHost, _Port},
                  sockname := {_Host, SockPort}}, Options) ->

    %% 1. call driver:init(conn(), conninfo())
    %% 2. got a state()
    %% 3. return the channel_state()
    #channel{driver = todo, state = todo, conninfo = todo}.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(binary(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_in(Data, Channel) ->
    %% 1. dispath data to driver
    %% 2. save new driver state
    %% 3. return updated channel
    {ok, Channel#channel{state = newstate}}.

-spec(handle_deliver(list(emqx_types:deliver()), channel())
      -> {ok, channe()}
       | {shutdown, Reason :: term(), channel()}).

handle_deliver(Delivers, Channel) ->
    %% TODO: ?? Nack delivers from shared subscription

    %% 1. dispath data to driver
    %% 2. save new driver state
    %% 3. return updated channel
    {ok, Channel#channel{state = newstate}}.


handle_timeout() ->
   %% TODO: ??
   ok.

handle_call() ->
    ok.

handle_info() ->
    ok.

terminate() ->
    ok.

