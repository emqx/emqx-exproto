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

-include("emqx_exproto.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
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
        , handle_cast/2
        , handle_info/2
        , terminate/2
        ]).

-export_type([channel/0]).

-record(channel, {
          %% gRPC channel options
          gcli :: map(),
          %% Conn info
          conninfo :: emqx_types:conninfo(),
          %% Client info from `register` function
          clientinfo :: maybe(map()),
          %% Connection state
          conn_state :: conn_state(),
          %% Subscription
          subscriptions = #{},
          %% Request queue
          rqueue = queue:new(),
          %% Inflight function name
          inflight = undefined,
          %% Keepalive
          keepalive :: maybe(emqx_keepalive:keepalive()),
          %% Timers
          timers ::  #{atom() => disabled | maybe(reference())},
          %% Closed reason
          closed_reason = undefined
         }).

-opaque(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-type(reply() :: {outgoing, binary()}
               | {outgoing, [binary()]}
               | {close, Reason :: atom()}).

-type(replies() :: emqx_types:packet() | reply() | [reply()]).

-define(TIMER_TABLE, #{
          alive_timer => keepalive,
          force_timer => force_close
         }).

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
info(session, #channel{subscriptions = Subs,
                       conninfo = ConnInfo}) ->
    #{subscriptions => Subs,
      upgrade_qos => false,
      retry_interval => 0,
      await_rel_timeout => 0,
      created_at => maps:get(connected_at, ConnInfo)};
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(will_msg, _) ->
    undefined.

-spec(stats(channel()) -> emqx_types:stats()).
stats(#channel{subscriptions = Subs}) ->
    [{subscriptions_cnt, maps:size(Subs)},
     {subscriptions_max, 0},
     {inflight_cnt, 0},
     {inflight_max, 0},
     {mqueue_len, 0},
     {mqueue_max, 0},
     {mqueue_dropped, 0},
     {next_pkt_id, 0},
     {awaiting_rel_cnt, 0},
     {awaiting_rel_max, 0}].

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec(init(emqx_exproto_types:conninfo(), proplists:proplist()) -> channel()).
init(ConnInfo = #{socktype := Socktype,
                  peername := Peername,
                  sockname := Sockname,
                  peercert := Peercert}, Options) ->
    GRpcChann = proplists:get_value(handler, Options),
    NConnInfo = default_conninfo(ConnInfo),
    ClientInfo = default_clientinfo(ConnInfo),
    Channel = #channel{gcli = #{channel => GRpcChann},
                       conninfo = NConnInfo,
                       clientinfo = ClientInfo,
                       conn_state = connecting,
                       timers = #{}
                      },

    Req = #{conninfo =>
            peercert(Peercert,
                     #{socktype => socktype(Socktype),
                       peername => address(Peername),
                       sockname => address(Sockname)})},
    try_dispatch(on_socket_created, wrap(Req), Channel).

%% @private
peercert(nossl, ConnInfo) ->
    ConnInfo;
peercert(Peercert, ConnInfo) ->
    ConnInfo#{peercert =>
              #{cn => esockd_peercert:common_name(Peercert),
                dn => esockd_peercert:subject(Peercert)}}.

%% @private
socktype(tcp) -> 'TCP';
socktype(ssl) -> 'SSL';
socktype(udp) -> 'UDP';
socktype(dtls) -> 'DTLS'.

%% @private
address({Host, Port}) ->
    #{host => inet:ntoa(Host), port => Port}.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(binary(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_in(Data, Channel) ->
    Req = #{bytes => Data},
    {ok, try_dispatch(on_received_bytes, wrap(Req), Channel)}.

-spec(handle_deliver(list(emqx_types:deliver()), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_deliver(Delivers, Channel = #channel{clientinfo = ClientInfo}) ->
    %% XXX: ?? Nack delivers from shared subscriptions
    Mountpoint = maps:get(mountpoint, ClientInfo),
    NodeStr = atom_to_binary(node(), utf8),
    Msgs = lists:map(fun({_, _, Msg}) ->
               ok = emqx_metrics:inc('messages.delivered'),
               Msg1 = emqx_hooks:run_fold('message.delivered',
                                          [ClientInfo], Msg),
               NMsg = emqx_mountpoint:unmount(Mountpoint, Msg1),
               #{node => NodeStr,
                 id => hexstr(emqx_message:id(NMsg)),
                 qos => emqx_message:qos(NMsg),
                 from => fmt_from(emqx_message:from(NMsg)),
                 topic => emqx_message:topic(NMsg),
                 payload => emqx_message:payload(NMsg),
                 timestamp => emqx_message:timestamp(NMsg)
               }
           end, Delivers),
    Req = #{messages => Msgs},
    {ok, try_dispatch(on_received_messages, wrap(Req), Channel)}.

-spec(handle_timeout(reference(), Msg :: term(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_timeout(_TRef, {keepalive, _StatVal},
               Channel = #channel{keepalive = undefined}) ->
    {ok, Channel};
handle_timeout(_TRef, {keepalive, StatVal},
               Channel = #channel{keepalive = Keepalive}) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            Req = #{type => 'KEEPALIVE'},
            {ok, try_dispatch(on_timer_timeout, wrap(Req), Channel)}
    end;

handle_timeout(_TRef, force_close, Channel = #channel{closed_reason = Reason}) ->
    {shutdown, {error, {force_close, Reason}}, Channel};

handle_timeout(_TRef, Msg, Channel) ->
    ?WARN("Unexpected timeout: ~p", [Msg]),
    {ok, Channel}.

-spec(handle_call(any(), channel())
     -> {reply, Reply :: term(), channel()}
      | {reply, Reply :: term(), replies(), channel()}
      | {shutdown, Reason :: term(), Reply :: term(), channel()}).

handle_call({send, Data}, Channel) ->
    {reply, ok, [{outgoing, Data}], Channel};

handle_call(close, Channel = #channel{conn_state = connected}) ->
    {reply, ok, [{event, disconnected}, {close, normal}], Channel};
handle_call(close, Channel) ->
    {reply, ok, [{close, normal}], Channel};

handle_call({auth, ClientInfo, _Password}, Channel = #channel{conn_state = connected}) ->
    ?LOG(warning, "Duplicated authorized command, dropped ~p", [ClientInfo]),
    {reply, {error, ?RESP_PERMISSION_DENY, <<"Duplicated authenticate command">>}, Channel};
handle_call({auth, ClientInfo0, Password},
            Channel = #channel{conninfo = ConnInfo,
                               clientinfo = ClientInfo}) ->
    ClientInfo1 = enrich_clientinfo(ClientInfo0, ClientInfo),
    NConnInfo = enrich_conninfo(ClientInfo1, ConnInfo),

    Channel1 = Channel#channel{conninfo = NConnInfo,
                               clientinfo = ClientInfo1},

    #{clientid := ClientId, username := Username} = ClientInfo1,

    case emqx_access_control:authenticate(ClientInfo1#{password => Password}) of
        {ok, AuthResult} ->
            emqx_logger:set_metadata_clientid(ClientId),
            is_anonymous(AuthResult) andalso
                emqx_metrics:inc('client.auth.anonymous'),
            NClientInfo = maps:merge(ClientInfo1, AuthResult),
            NChannel = Channel1#channel{clientinfo = NClientInfo},
            case emqx_cm:open_session(true, NClientInfo, NConnInfo) of
                {ok, _Session} ->
                    ?LOG(debug, "Client ~s (Username: '~s') authorized successfully!",
                                [ClientId, Username]),
                    {reply, ok, [{event, connected}], ensure_connected(NChannel)};
                {error, Reason} ->
                    ?LOG(warning, "Client ~s (Username: '~s') open session failed for ~0p",
                         [ClientId, Username, Reason]),
                    {reply, {error, ?RESP_PERMISSION_DENY, Reason}, Channel}
            end;
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                 [ClientId, Username, Reason]),
            {reply, {error, ?RESP_PERMISSION_DENY, Reason}, Channel}
    end;

handle_call({start_timer, keepalive, Interval},
            Channel = #channel{
                         conninfo = ConnInfo,
                         clientinfo = ClientInfo
                        }) ->
    NConnInfo = ConnInfo#{keepalive => Interval},
    NClientInfo = ClientInfo#{keepalive => Interval},
    NChannel = Channel#channel{conninfo = NConnInfo, clientinfo = NClientInfo},
    {reply, ok, ensure_keepalive(NChannel)};

handle_call({subscribe, TopicFilter, Qos},
            Channel = #channel{
                         conn_state = connected,
                         clientinfo = ClientInfo}) ->
    case is_acl_enabled(ClientInfo) andalso
         emqx_access_control:check_acl(ClientInfo, subscribe, TopicFilter) of
        deny ->
            {reply, {error, ?RESP_PERMISSION_DENY, <<"ACL deny">>}, Channel};
        _ ->
            {ok, NChannel} = do_subscribe([{TopicFilter, #{qos => Qos}}], Channel),
            {reply, ok, NChannel}
    end;

handle_call({unsubscribe, TopicFilter},
            Channel = #channel{conn_state = connected}) ->
    {ok, NChannel} = do_unsubscribe([{TopicFilter, #{}}], Channel),
    {reply, ok, NChannel};

handle_call({publish, Topic, Qos, Payload},
            Channel = #channel{
                         conn_state = connected,
                         clientinfo = ClientInfo
                                    = #{clientid := From,
                                        mountpoint := Mountpoint}}) ->
    case is_acl_enabled(ClientInfo) andalso
         emqx_access_control:check_acl(ClientInfo, publish, Topic) of
        deny ->
            {reply, {error, ?RESP_PERMISSION_DENY, <<"ACL deny">>}, Channel};
        _ ->
            Msg = emqx_message:make(From, Qos, Topic, Payload),
            NMsg = emqx_mountpoint:mount(Mountpoint, Msg),
            emqx:publish(NMsg),
            {reply, ok, Channel}
    end;

handle_call(kick, Channel) ->
    {shutdown, kicked, ok, Channel};

handle_call(Req, Channel) ->
    ?LOG(warning, "Unexpected call: ~p", [Req]),
    {reply, {error, unexpected_call}, Channel}.

-spec(handle_cast(any(), channel())
     -> {ok, channel()}
      | {ok, replies(), channel()}
      | {shutdown, Reason :: term(), channel()}).
handle_cast(Req, Channel) ->
    ?WARN("Unexpected call: ~p", [Req]),
    {ok, Channel}.

-spec(handle_info(any(), channel())
      -> {ok, channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_info({subscribe, TopicFilters}, Channel) ->
    do_subscribe(TopicFilters, Channel);

handle_info({unsubscribe, TopicFilters}, Channel) ->
    do_unsubscribe(TopicFilters, Channel);

handle_info({sock_closed, Reason},
            Channel = #channel{rqueue = Queue, inflight = Inflight}) ->
    case queue:len(Queue) =:= 0
         andalso Inflight =:= undefined of
        true ->
            {shutdown, {sock_closed, Reason}, Channel};
        _ ->
            %% delayed close process for flushing all callback funcs to gRPC server
            Channel1 = Channel#channel{closed_reason = {sock_closed, Reason}},
            Channel2 = ensure_timer(force_timer, Channel1),
            {ok, ensure_disconnected({sock_closed, Reason}, Channel2)}
    end;

handle_info({hreply, on_socket_created, {ok, _}}, Channel) ->
    dispatch_or_close_process(Channel#channel{inflight = undefined});
handle_info({hreply, FunName, {ok, _}}, Channel)
  when FunName == on_socket_closed;
       FunName == on_received_bytes;
       FunName == on_received_messages;
       FunName == on_timer_timeout ->
    dispatch_or_close_process(Channel#channel{inflight = undefined});
handle_info({hreply, FunName, {error, Reason}}, Channel) ->
    {shutdown, {error, {FunName, Reason}}, Channel};

handle_info(Info, Channel) ->
    ?LOG(warning, "Unexpected info: ~p", [Info]),
    {ok, Channel}.

-spec(terminate(any(), channel()) -> channel()).
terminate(Reason, Channel) ->
    Req = #{reason => stringfy(Reason)},
    try_dispatch(on_socket_closed, wrap(Req), Channel).

is_anonymous(#{anonymous := true}) -> true;
is_anonymous(_AuthResult)          -> false.

%%--------------------------------------------------------------------
%% Sub/UnSub
%%--------------------------------------------------------------------

do_subscribe(TopicFilters, Channel) ->
    NChannel = lists:foldl(
        fun({TopicFilter, SubOpts}, ChannelAcc) ->
            do_subscribe(TopicFilter, SubOpts, ChannelAcc)
        end, Channel, parse_topic_filters(TopicFilters)),
    {ok, NChannel}.

%% @private
do_subscribe(TopicFilter, SubOpts, Channel =
             #channel{clientinfo = ClientInfo = #{mountpoint := Mountpoint},
                      subscriptions = Subs}) ->
    %% Mountpoint first
    NTopicFilter = emqx_mountpoint:mount(Mountpoint, TopicFilter),
    NSubOpts = maps:merge(?DEFAULT_SUBOPTS, SubOpts),
    SubId = maps:get(clientid, ClientInfo, undefined),
    IsNew = not maps:is_key(NTopicFilter, Subs),
    case IsNew of
        true ->
            ok = emqx:subscribe(NTopicFilter, SubId, NSubOpts),
            ok = emqx_hooks:run('session.subscribed',
                                [ClientInfo, NTopicFilter, NSubOpts#{is_new => IsNew}]),
            Channel#channel{subscriptions = Subs#{NTopicFilter => NSubOpts}};
        _ ->
            %% Update subopts
            ok = emqx:subscribe(NTopicFilter, SubId, NSubOpts),
            Channel#channel{subscriptions = Subs#{NTopicFilter => NSubOpts}}
    end.

do_unsubscribe(TopicFilters, Channel) ->
    NChannel = lists:foldl(
        fun({TopicFilter, SubOpts}, ChannelAcc) ->
            do_unsubscribe(TopicFilter, SubOpts, ChannelAcc)
        end, Channel, parse_topic_filters(TopicFilters)),
    {ok, NChannel}.

%% @private
do_unsubscribe(TopicFilter, UnSubOpts, Channel =
               #channel{clientinfo = ClientInfo = #{mountpoint := Mountpoint},
                        subscriptions = Subs}) ->
    NTopicFilter = emqx_mountpoint:mount(Mountpoint, TopicFilter),
    case maps:find(NTopicFilter, Subs) of
        {ok, SubOpts} ->
            ok = emqx:unsubscribe(NTopicFilter),
            ok = emqx_hooks:run('session.unsubscribed',
                                [ClientInfo, TopicFilter, maps:merge(SubOpts, UnSubOpts)]),
            Channel#channel{subscriptions = maps:remove(NTopicFilter, Subs)};
        _ ->
            Channel
    end.

%% @private
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

-compile({inline, [is_acl_enabled/1]}).
is_acl_enabled(#{zone := Zone, is_superuser := IsSuperuser}) ->
    (not IsSuperuser) andalso emqx_zone:enable_acl(Zone).

%%--------------------------------------------------------------------
%% Ensure & Hooks
%%--------------------------------------------------------------------

ensure_connected(Channel = #channel{conninfo = ConnInfo,
                                    clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{conninfo   = NConnInfo,
                    conn_state = connected
                   }.

ensure_disconnected(Reason, Channel = #channel{
                                         conn_state = connected,
                                         conninfo = ConnInfo,
                                         clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo]),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected};

ensure_disconnected(_Reason, Channel = #channel{conninfo = ConnInfo}) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run(Name, Args).

%%--------------------------------------------------------------------
%% Enrich Keepalive

ensure_keepalive(Channel = #channel{clientinfo = ClientInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ClientInfo, 0), Channel).

ensure_keepalive_timer(Interval, Channel) when Interval =< 0 ->
    Channel;
ensure_keepalive_timer(Interval, Channel) ->
    Keepalive = emqx_keepalive:init(timer:seconds(Interval)),
    ensure_timer(alive_timer, Channel#channel{keepalive = Keepalive}).

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true  -> ensure_timer(Name, Time, Channel);
        false -> Channel %% Timer disabled or exists
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_misc:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(force_timer, _) ->
    15000;
interval(alive_timer, #channel{keepalive = Keepalive}) ->
    emqx_keepalive:info(interval, Keepalive).

%%--------------------------------------------------------------------
%% Dispatch
%%--------------------------------------------------------------------

wrap(Req) ->
     Req#{conn => pid_to_list(self())}.

dispatch_or_close_process(Channel = #channel{
                                       rqueue = Queue,
                                       inflight = undefined,
                                       gcli = GClient}) ->
    case queue:out(Queue) of
        {empty, _} ->
            case Channel#channel.conn_state of
                disconnected ->
                    {shutdown, Channel#channel.closed_reason, Channel};
                _ ->
                    {ok, Channel}
            end;
        {{value, {FunName, Req}}, NQueue} ->
            emqx_exproto_gcli:async_call(FunName, Req, GClient),
            {ok, Channel#channel{inflight = FunName, rqueue = NQueue}}
    end.

try_dispatch(FunName, Req, Channel = #channel{inflight = undefined, gcli = GClient}) ->
    emqx_exproto_gcli:async_call(FunName, Req, GClient),
    Channel#channel{inflight = FunName};
try_dispatch(FunName, Req, Channel = #channel{rqueue = Queue}) ->
    Channel#channel{rqueue = queue:in({FunName, Req}, Queue)}.

%%--------------------------------------------------------------------
%% Format
%%--------------------------------------------------------------------

enrich_conninfo(InClientInfo, ConnInfo) ->
    Ks = [proto_name, proto_ver, clientid, username],
    maps:merge(ConnInfo, maps:with(Ks, InClientInfo)).

enrich_clientinfo(InClientInfo = #{proto_name := ProtoName}, ClientInfo) ->
    Ks = [clientid, username, mountpoint],
    NClientInfo = maps:merge(ClientInfo, maps:with(Ks, InClientInfo)),
    NClientInfo#{protocol => ProtoName}.

default_conninfo(ConnInfo) ->
    ConnInfo#{proto_name => undefined,
              proto_ver => undefined,
              clean_start => true,
              clientid => undefined,
              username => undefined,
              conn_props => #{},
              connected => true,
              connected_at => erlang:system_time(millisecond),
              keepalive => undefined,
              receive_maximum => 0,
              expiry_interval => 0}.

default_clientinfo(#{peername := {PeerHost, _},
                     sockname := {_, SockPort}}) ->
    #{zone         => external,
      protocol     => undefined,
      peerhost     => PeerHost,
      sockport     => SockPort,
      clientid     => undefined,
      username     => undefined,
      is_bridge    => false,
      is_superuser => false,
      mountpoint   => undefined}.

stringfy(Reason) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Reason]))).

hexstr(Bin) ->
    [io_lib:format("~2.16.0B",[X]) || <<X:8>> <= Bin].

fmt_from(undefined) -> <<>>;
fmt_from(Bin) when is_binary(Bin) -> Bin;
fmt_from(T) -> stringfy(T).
