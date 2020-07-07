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

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

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

-export_type([channel/0]).

-record(channel, {
          %% Driver
          driver :: emqx_exproto_driver_mnger:driver(),
          %% Socket connInfo
          conninfo :: emqx_types:conninfo(),
          %% Client info from `register` function
          clientinfo :: maybe(map()),
          %% Connection state
          conn_state :: conn_state(),
          %% Subscription
          subscription :: map(),
          %% Driver level state
          state :: any()
         }).

-opaque(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

-define(SESSION_STATS_KEYS,
        [subscriptions_cnt,
         subscriptions_max,
         inflight_cnt,
         inflight_max,
         mqueue_len,
         mqueue_max,
         mqueue_dropped,
         next_pkt_id,
         awaiting_rel_cnt,
         awaiting_rel_max
        ]).

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
info(clientid, #channel{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, _) ->
    undefined;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(will_msg, _) ->
    undefined.

-spec(stats(channel()) -> emqx_types:stats()).
stats(_Channel) ->
    %% XXX:
    [].

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
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_deliver(Delivers, Channel) ->
    %% TODO: ?? Nack delivers from shared subscription
    %% 1. dispath data to driver
    %% 2. save new driver state
    %% 3. return updated channel
    {ok, Channel#channel{state = newstate}}.

-spec(handle_timeout(reference(), Msg :: term(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_timeout(_TRef, Msg, Channel) ->
    ?WARN("Unexpected timeout: ~p", [Msg]),
    {ok, Channel}.

-spec(handle_call(any(), channel())
     -> {reply, Reply :: term(), channel()}
      | {shutdown, Reason :: term(), Reply :: term(), channel()}).
handle_call({register, ClientInfo}, Channel) ->
    %% 1. Register clientid and evict client used same clientid
    %% 2. Return new Channel
    {reply, ok, Channel#channel{clientinfo = ClientInfo}};

handle_call({subscribe, _Topic, _SubOpts}, Channel) ->
    %% 1. Execute subscribe operation
    %% 2. Save the subscription??
    %% 3. Return new channel
    {reply, ok, Channel};

handle_call({publish, Msg}, Channel) ->
    %% 1. Execute publish operation
    %% 2. Return new channel
    {reply, ok, Channel};

handle_call(Req, Channel) ->
    ?WARN("Unexpected call: ~p", [Req]),
    {reply, ok, Channel}.

-spec(handle_info(any(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_info(_Info, _Channel) ->
    ok.

-spec(terminate(any(), channel()) -> ok).
terminate(_Reason, _Channel) ->
    ok.

%%--------------------------------------------------------------------
%% Cbs for driver
%%--------------------------------------------------------------------

cb_init(Channel = #channel{conninfo = ConnInfo}) ->
    Args = [self(), format(ConnInfo)],
    do_call_cb('init', Args, Channel).

cb_recevied(Data, Channel = #channel{state = DState}) ->
    Args = [self(), Data, DState],
    do_call_cb('recevied', Args, Channel).

cb_terminated(Reason, Channel = #channel{state = DState}) ->
    Args = [self(), format(Reason), DState],
    do_call_cb('terminated', Args, Channel).

cb_deliver(Delivers, Channel = #channel{state = DState}) ->
    Args = [self(), format(Delivers), DState],
    do_call_cb('deliver', Args, Channel).

%% @private
do_call_cb(Fun, Args, Channel = #channel{driver = D}) ->
    case emqx_exproto_driver_mnger:call(D, {Fun, Args}) of
        {ok, NDState} ->
            {ok, Channel#channel{state = NDState}};
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Format
%%--------------------------------------------------------------------

format(T) -> T.
