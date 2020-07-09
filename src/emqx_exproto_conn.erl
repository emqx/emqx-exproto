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

%% TCP/TLS/UDP/DTLS Connection
-module(emqx_exproto_conn).

-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[ExProto Conn]").

%% API
-export([ start_link/3
        , stop/1
        ]).

-export([ info/1
        , stats/1
        ]).

-export([call/2]).

%% Callback
-export([init/4]).

%% Sys callbacks
-export([ system_continue/3
        , system_terminate/4
        , system_code_change/4
        , system_get_state/1
        ]).

%% Internal callback
-export([wakeup_from_hib/2]).

-import(emqx_misc, [start_timer/2]).

-record(state, {
          %% TCP/SSL/UDP/DTLS Wrapped Socket
          socket :: esockd:socket(),
          %% Peername of the connection
          peername :: emqx_types:peername(),
          %% Sockname of the connection
          sockname :: emqx_types:peername(),
          %% Sock State
          sockstate :: emqx_types:sockstate(),
          %% The {active, N} option
          active_n :: pos_integer(),
          %% Send function
          sendfun :: function(),
          %% Limiter
          limiter :: maybe(emqx_limiter:limiter()),
          %% Limit Timer
          limit_timer :: maybe(reference()),
          %% Channel State
          channel :: emqx_exproto_channel:channel(),
          %% GC State
          gc_state :: maybe(emqx_gc:gc_state()),
          %% Stats Timer
          stats_timer :: disabled | maybe(reference()),
          %% Idle Timeout
          idle_timeout :: integer(),
          %% Idle Timer
          idle_timer :: maybe(reference())
        }).

-type(state() :: #state{}).

-define(ACTIVE_N, 100).
-define(INFO_KEYS, [socktype, peername, sockname, sockstate, active_n]).
-define(CONN_STATS, [recv_pkt, recv_msg, send_pkt, send_msg]).
-define(SOCK_STATS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).

-define(ENABLED(X), (X =/= undefined)).

-dialyzer({nowarn_function,
           [ system_terminate/4
           ]}).

%% udp
start_link(Socket = {udp, _SockPid, _Sock}, Peername, Options) ->
    Args = [self(), Socket, Peername, Options],
    {ok, proc_lib:spawn_link(?MODULE, init, [Args])};

%% tcp/ssl/dtls
start_link(esockd_transport, Sock, Options) ->
    Socket = {esockd_transport, Sock},
    case esockd_transport:peername(Sock) of
        {ok, Peername} ->
            Args = [self(), Socket, Peername, Options],
            {ok, proc_lib:spawn_link(?MODULE, init, [Args])};
        R = {error, _} -> R
    end.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Get infos of the connection/channel.
-spec(info(pid()|state()) -> emqx_types:infos()).
info(CPid) when is_pid(CPid) ->
    call(CPid, info);
info(State = #state{channel = Channel}) ->
    ChanInfo = emqx_exproto_channel:info(Channel),
    SockInfo = maps:from_list(
                 info(?INFO_KEYS, State)),
    ChanInfo#{sockinfo => SockInfo}.

info(Keys, State) when is_list(Keys) ->
    [{Key, info(Key, State)} || Key <- Keys];
info(socktype, #state{socket = Socket}) ->
    esockd_type(Socket);
info(peername, #state{peername = Peername}) ->
    Peername;
info(sockname, #state{sockname = Sockname}) ->
    Sockname;
info(sockstate, #state{sockstate = SockSt}) ->
    SockSt;
info(active_n, #state{active_n = ActiveN}) ->
    ActiveN.

-spec(stats(pid()|state()) -> emqx_types:stats()).
stats(CPid) when is_pid(CPid) ->
    call(CPid, stats);
stats(#state{socket  = Socket,
             channel = Channel}) ->
    SockStats = case esockd_getstat(Socket, ?SOCK_STATS) of
                    {ok, Ss}   -> Ss;
                    {error, _} -> []
                end,
    ConnStats = emqx_pd:get_counters(?CONN_STATS),
    ChanStats = emqx_exproto_channel:stats(Channel),
    ProcStats = emqx_misc:proc_stats(),
    lists:append([SockStats, ConnStats, ChanStats, ProcStats]).

call(Pid, Req) ->
    gen_server:call(Pid, Req, infinity).

stop(Pid) ->
    gen_server:stop(Pid).

%%--------------------------------------------------------------------
%% Wrapped funcs
%%--------------------------------------------------------------------

esockd_wait(Socket = {udp, _SockPid, _Sock}) ->
    {ok, Socket};
esockd_wait({esockd_transport, Sock}) ->
    case esockd_transport:wait(Sock) of
        {ok, NSock} -> {ok, {esockd_transport, NSock}};
        R = {error, _} -> R
    end.

esockd_close({udp, _SockPid, Sock}) ->
    gen_udp:close(Sock);
esockd_close({esockd_transport, Sock}) ->
    esockd_transport:fast_close(Sock).

esockd_ensure_ok_or_exit(peercert, [{udp, _SockPid, _Sock}]) ->
    nossl;
esockd_ensure_ok_or_exit(Fun, [{udp, _SockPid, Sock}]) ->
    esockd_transport:ensure_ok_or_exit(Fun, Sock);
esockd_ensure_ok_or_exit(Fun, [{esockd_transport, Socket}]) ->
    esockd_transport:ensure_ok_or_exit(Fun, Socket).

esockd_type({udp, _, _}) ->
    udp;
esockd_type({esockd_transport, Socket}) ->
    esockd_transport:type(Socket).

esockd_setopts({udp, _, _}, _) ->
    ok;
esockd_setopts({esockd_transport, Socket}, Opts) ->
    %% FIXME: DTLS works??
    esockd_transport:setopts(Socket, Opts).

esockd_getstat({udp, _SockPid, Sock}, Stats) ->
    inet:getstat(Sock, Stats);
esockd_getstat({esockd_transport, Sock}, Stats) ->
    esockd_transport:getstat(Sock, Stats).



sendfun({udp, _SockPid, Sock}, {Ip, Port}) ->
    fun(Data) ->
        gen_udp:send(Sock, Ip, Port, Data)
    end;
sendfun({esockd_transport, Sock}, _) ->
    fun(Data) ->
        esockd_transport:async_send(Sock, Data)
    end.

%%--------------------------------------------------------------------
%% callbacks
%%--------------------------------------------------------------------

-define(DEFAULT_GC_OPTS, #{count => 1000, bytes => 1024*1024}).
-define(DEFAULT_IDLE_TIMEOUT, 30000).
-define(DEFAULT_OOM_POLICY, #{max_heap_size => 4194304,message_queue_len => 32000}).

init(Parent, WrappedSock, _Peername, Options) ->
    case esockd_wait(WrappedSock) of
        {ok, NWrappedSock} ->
            run_loop(Parent, init_state(NWrappedSock, Options));
        {error, Reason} ->
            ok = esockd_close(WrappedSock),
            exit_on_sock_error(Reason)
    end.

init_state(WrappedSock, Options) ->
    {ok, Peername} = esockd_ensure_ok_or_exit(peername, [WrappedSock]),
    {ok, Sockname} = esockd_ensure_ok_or_exit(sockname, [WrappedSock]),
    Peercert = esockd_ensure_ok_or_exit(peercert, [WrappedSock]),
    ConnInfo = #{socktype => esockd_type(WrappedSock),
                 peername => Peername,
                 sockname => Sockname,
                 peercert => Peercert,
                 conn_mod => ?MODULE
                },

    ActiveN = proplists:get_value(active_n, Options, ?ACTIVE_N),

    Limiter = emqx_limiter:init(Options),

    Channel = emqx_exproto_channel:init(ConnInfo, Options),

    GcState = emqx_gc:init(?DEFAULT_GC_OPTS),

    IdleTimeout = proplists:get_value(idle_timeout, Options, ?DEFAULT_IDLE_TIMEOUT),
    IdleTimer = start_timer(IdleTimeout, idle_timeout),
    #state{socket       = WrappedSock,
           peername     = Peername,
           sockname     = Sockname,
           sockstate    = idle,
           active_n     = ActiveN,
           sendfun      = sendfun(WrappedSock, Peername),
           limiter      = Limiter,
           channel      = Channel,
           gc_state     = GcState,
           stats_timer  = disabled,
           idle_timeout = IdleTimeout,
           idle_timer   = IdleTimer
          }.

run_loop(Parent, State = #state{socket   = Socket,
                                peername = Peername}) ->
    emqx_logger:set_metadata_peername(esockd:format(Peername)),
    emqx_misc:tune_heap_size(?DEFAULT_OOM_POLICY),
    case activate_socket(State) of
        {ok, NState} ->
            hibernate(Parent, NState);
        {error, Reason} ->
            ok = esockd_close(Socket),
            exit_on_sock_error(Reason)
    end.

exit_on_sock_error(Reason) when Reason =:= einval;
                                Reason =:= enotconn;
                                Reason =:= closed ->
    erlang:exit(normal);
exit_on_sock_error(timeout) ->
    erlang:exit({shutdown, ssl_upgrade_timeout});
exit_on_sock_error(Reason) ->
    erlang:exit({shutdown, Reason}).

%%--------------------------------------------------------------------
%% Recv Loop

recvloop(Parent, State = #state{idle_timeout = IdleTimeout}) ->
    receive
        {system, From, Request} ->
            sys:handle_system_msg(Request, From, Parent, ?MODULE, [], State);
        {'EXIT', Parent, Reason} ->
            terminate(Reason, State);
        Msg ->
            process_msg([Msg], Parent, ensure_stats_timer(IdleTimeout, State))
    after
        IdleTimeout ->
            hibernate(Parent, cancel_stats_timer(State))
    end.

hibernate(Parent, State) ->
    proc_lib:hibernate(?MODULE, wakeup_from_hib, [Parent, State]).

%% Maybe do something here later.
wakeup_from_hib(Parent, State) -> recvloop(Parent, State).

%%--------------------------------------------------------------------
%% Ensure/cancel stats timer

-compile({inline, [ensure_stats_timer/2]}).
ensure_stats_timer(Timeout, State = #state{stats_timer = undefined}) ->
    State#state{stats_timer = start_timer(Timeout, emit_stats)};
ensure_stats_timer(_Timeout, State) -> State.

-compile({inline, [cancel_stats_timer/1]}).
cancel_stats_timer(State = #state{stats_timer = TRef}) when is_reference(TRef) ->
    ok = emqx_misc:cancel_timer(TRef),
    State#state{stats_timer = undefined};
cancel_stats_timer(State) -> State.

%%--------------------------------------------------------------------
%% Process next Msg

process_msg([], Parent, State) -> recvloop(Parent, State);

process_msg([Msg|More], Parent, State) ->
    case catch handle_msg(Msg, State) of
        ok ->
            process_msg(More, Parent, State);
        {ok, NState} ->
            process_msg(More, Parent, NState);
        {ok, Msgs, NState} ->
            process_msg(append_msg(More, Msgs), Parent, NState);
        {stop, Reason} ->
            terminate(Reason, State);
        {stop, Reason, NState} ->
            terminate(Reason, NState);
        {'EXIT', Reason} ->
            terminate(Reason, State)
    end.

-compile({inline, [append_msg/2]}).
append_msg([], Msgs) when is_list(Msgs) ->
    Msgs;
append_msg([], Msg) -> [Msg];
append_msg(Q, Msgs) when is_list(Msgs) ->
    lists:append(Q, Msgs);
append_msg(Q, Msg) ->
    lists:append(Q, [Msg]).

%%--------------------------------------------------------------------
%% Handle a Msg

handle_msg({'$gen_call', From, Req}, State) ->
    case handle_call(From, Req, State) of
        {reply, Reply, NState} ->
            gen_server:reply(From, Reply),
            {ok, NState};
        {stop, Reason, Reply, NState} ->
            gen_server:reply(From, Reply),
            stop(Reason, NState)
    end;

handle_msg({datagram, _SockPid, Data}, State) ->
    process_incoming(Data, State);

handle_msg({Inet, _Sock, Data}, State) when Inet == tcp; Inet == ssl ->
    process_incoming(Data, State);

handle_msg({outgoing, Data}, State) ->
    handle_outgoing(Data, State);

handle_msg({Error, _Sock, Reason}, State)
  when Error == tcp_error; Error == ssl_error ->
    handle_info({sock_error, Reason}, State);

handle_msg({Closed, _Sock}, State)
  when Closed == tcp_closed; Closed == ssl_closed ->
    handle_info({sock_closed, Closed}, close_socket(State));

%% TODO: udp_passive???
handle_msg({Passive, _Sock}, State)
  when Passive == tcp_passive; Passive == ssl_passive ->
    %% In Stats
    Bytes = emqx_pd:reset_counter(incoming_bytes),
    Pubs = emqx_pd:reset_counter(incoming_pkt),
    InStats = #{cnt => Pubs, oct => Bytes},
    %% Ensure Rate Limit
    NState = ensure_rate_limit(InStats, State),
    %% Run GC and Check OOM
    NState1 = check_oom(run_gc(InStats, NState)),
    handle_info(activate_socket, NState1);

handle_msg(Deliver = {deliver, _Topic, _Msg},
           State = #state{active_n = ActiveN}) ->
    Delivers = [Deliver|emqx_misc:drain_deliver(ActiveN)],
    with_channel(handle_deliver, [Delivers], State);

%% Something sent
%% TODO: Who will deliver this message?
handle_msg({inet_reply, _Sock, ok}, State = #state{active_n = ActiveN}) ->
    case emqx_pd:get_counter(outgoing_pkt) > ActiveN of
        true ->
            Pubs = emqx_pd:reset_counter(outgoing_pkt),
            Bytes = emqx_pd:reset_counter(outgoing_bytes),
            OutStats = #{cnt => Pubs, oct => Bytes},
            {ok, check_oom(run_gc(OutStats, State))};
        false -> ok
    end;

handle_msg({inet_reply, _Sock, {error, Reason}}, State) ->
    handle_info({sock_error, Reason}, State);

handle_msg({close, Reason}, State) ->
    ?LOG(debug, "Force to close the socket due to ~p", [Reason]),
    handle_info({sock_closed, Reason}, close_socket(State));

%handle_msg({event, connected}, State = #state{channel = Channel}) ->
%    ClientId = emqx_exproto_channel:info(clientid, Channel),
%    emqx_cm:register_channel(ClientId, info(State), stats(State));
%
%handle_msg({event, disconnected}, State = #state{channel = Channel}) ->
%    ClientId = emqx_exproto_channel:info(clientid, Channel),
%    emqx_cm:set_chan_info(ClientId, info(State)),
%    emqx_cm:connection_closed(ClientId),
%    {ok, State};

%handle_msg({event, _Other}, State = #state{channel = Channel}) ->
%    ClientId = emqx_exproto_channel:info(clientid, Channel),
%    emqx_cm:set_chan_info(ClientId, info(State)),
%    emqx_cm:set_chan_stats(ClientId, stats(State)),
%    {ok, State};

handle_msg({timeout, TRef, TMsg}, State) ->
    handle_timeout(TRef, TMsg, State);

handle_msg(Shutdown = {shutdown, _Reason}, State) ->
    stop(Shutdown, State);

handle_msg(Msg, State) ->
    handle_info(Msg, State).

%%--------------------------------------------------------------------
%% Terminate

terminate(Reason, State = #state{channel = Channel}) ->
    ?LOG(debug, "Terminated due to ~p", [Reason]),
    emqx_exproto_channel:terminate(Reason, Channel),
    close_socket(State),
    exit(Reason).

%%--------------------------------------------------------------------
%% Sys callbacks

system_continue(Parent, _Debug, State) ->
    recvloop(Parent, State).

system_terminate(Reason, _Parent, _Debug, State) ->
    terminate(Reason, State).

system_code_change(State, _Mod, _OldVsn, _Extra) ->
    {ok, State}.

system_get_state(State) -> {ok, State}.

%%--------------------------------------------------------------------
%% Handle call

handle_call(_From, info, State) ->
    {reply, info(State), State};

handle_call(_From, stats, State) ->
    {reply, stats(State), State};

handle_call(_From, Req, State = #state{channel = Channel}) ->
    case emqx_exproto_channel:handle_call(Req, Channel) of
        {reply, Reply, NChannel} ->
            {reply, Reply, State#state{channel = NChannel}};
        {shutdown, Reason, Reply, NChannel} ->
            shutdown(Reason, Reply, State#state{channel = NChannel})
    end.

%%--------------------------------------------------------------------
%% Handle timeout

handle_timeout(_TRef, idle_timeout, State) ->
    shutdown(idle_timeout, State);

handle_timeout(_TRef, limit_timeout, State) ->
    NState = State#state{sockstate   = idle,
                         limit_timer = undefined
                        },
    handle_info(activate_socket, NState);

handle_timeout(_TRef, emit_stats, State =
               #state{channel = Channel}) ->
    ClientId = emqx_exproto_channel:info(clientid, Channel),
    emqx_cm:set_chan_stats(ClientId, stats(State)),
    {ok, State#state{stats_timer = undefined}};

handle_timeout(TRef, Msg, State) ->
    with_channel(handle_timeout, [TRef, Msg], State).

%%--------------------------------------------------------------------
%% Parse incoming data

-compile({inline, [process_incoming/2]}).
process_incoming(Data, State = #state{idle_timer = IdleTimer}) ->
    ?LOG(debug, "RECV ~0p", [Data]),
    Oct = iolist_size(Data),
    emqx_pd:inc_counter(incoming_bytes, Oct),
    emqx_pd:inc_counter(incoming_pkt, 1),
    emqx_pd:inc_counter(recv_pkt, 1),
    emqx_pd:inc_counter(recv_msg, 1),
    % TODO:
    %ok = emqx_metrics:inc('bytes.received', Oct),

    ok = emqx_misc:cancel_timer(IdleTimer),
    NState = State#state{idle_timer = undefined},

    with_channel(handle_in, [Data], NState).

%%--------------------------------------------------------------------
%% With Channel

with_channel(Fun, Args, State = #state{channel = Channel}) ->
    case erlang:apply(emqx_exproto_channel, Fun, Args ++ [Channel]) of
        ok -> {ok, State};
        {ok, NChannel} ->
            {ok, State#state{channel = NChannel}};
        {ok, Replies, NChannel} ->
            {ok, next_msgs(Replies), State#state{channel = NChannel}};
        {shutdown, Reason, NChannel} ->
            shutdown(Reason, State#state{channel = NChannel})
        %{shutdown, Reason, Packet, NChannel} ->
        %    NState = State#state{channel = NChannel},
        %    ok = handle_outgoing(Packet, NState),
        %    shutdown(Reason, NState)
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packets

handle_outgoing(IoData, #state{socket = Socket, sendfun = SendFun}) ->
    ?LOG(debug, "SEND ~0p", [IoData]),

    Oct = iolist_size(IoData),

    emqx_pd:inc_counter(send_pkt, 1),
    emqx_pd:inc_counter(send_msg, 1),
    emqx_pd:inc_counter(outgoing_pkt, 1),
    emqx_pd:inc_counter(outgoing_bytes, Oct),

    %% FIXME:
    %%ok = emqx_metrics:inc('bytes.sent', Oct),
    case SendFun(IoData) of
        ok -> ok;
        Error = {error, _Reason} ->
            %% Send an inet_reply to postpone handling the error
            self() ! {inet_reply, Socket, Error},
            ok
    end.

%%--------------------------------------------------------------------
%% Handle Info

handle_info(activate_socket, State = #state{sockstate = OldSst}) ->
    case activate_socket(State) of
        {ok, NState = #state{sockstate = NewSst}} ->
            if OldSst =/= NewSst ->
                   {ok, {event, NewSst}, NState};
               true -> {ok, NState}
            end;
        {error, Reason} ->
            handle_info({sock_error, Reason}, State)
    end;

handle_info({sock_error, Reason}, State) ->
    ?LOG(debug, "Socket error: ~p", [Reason]),
    handle_info({sock_closed, Reason}, close_socket(State));

handle_info(Info, State) ->
    with_channel(handle_info, [Info], State).

%%--------------------------------------------------------------------
%% Ensure rate limit

ensure_rate_limit(Stats, State = #state{limiter = Limiter}) ->
    case ?ENABLED(Limiter) andalso emqx_limiter:check(Stats, Limiter) of
        false -> State;
        {ok, Limiter1} ->
            State#state{limiter = Limiter1};
        {pause, Time, Limiter1} ->
            ?LOG(warning, "Pause ~pms due to rate limit", [Time]),
            TRef = start_timer(Time, limit_timeout),
            State#state{sockstate   = blocked,
                        limiter     = Limiter1,
                        limit_timer = TRef
                       }
    end.

%%--------------------------------------------------------------------
%% Run GC and Check OOM

run_gc(Stats, State = #state{gc_state = GcSt}) ->
    case ?ENABLED(GcSt) andalso emqx_gc:run(Stats, GcSt) of
        false -> State;
        {_IsGC, GcSt1} ->
            State#state{gc_state = GcSt1}
    end.

check_oom(State) ->
    OomPolicy = ?DEFAULT_OOM_POLICY,
    case ?ENABLED(OomPolicy) andalso emqx_misc:check_oom(OomPolicy) of
        Shutdown = {shutdown, _Reason} ->
            erlang:send(self(), Shutdown);
        _Other -> ok
    end,
    State.

%%--------------------------------------------------------------------
%% Activate Socket

-compile({inline, [activate_socket/1]}).
activate_socket(State = #state{sockstate = closed}) ->
    {ok, State};
activate_socket(State = #state{sockstate = blocked}) ->
    {ok, State};
activate_socket(State = #state{socket   = Socket,
                               active_n = N}) ->
    %% FIXME: Works on dtls/udp ???
    %%        How to hanlde buffer?
    case esockd_setopts(Socket, [{active, N}]) of
        ok -> {ok, State#state{sockstate = running}};
        Error -> Error
    end.

%%--------------------------------------------------------------------
%% Close Socket

close_socket(State = #state{sockstate = closed}) -> State;
close_socket(State = #state{socket = Socket}) ->
    ok = esockd_close(Socket),
    State#state{sockstate = closed}.

%%--------------------------------------------------------------------
%% Helper functions

-compile({inline, [next_msgs/1]}).
next_msgs(Event) when is_tuple(Event) ->
    Event;
next_msgs(More) when is_list(More) ->
    More.

-compile({inline, [shutdown/2, shutdown/3]}).
shutdown(Reason, State) ->
    stop({shutdown, Reason}, State).

shutdown(Reason, Reply, State) ->
    stop({shutdown, Reason}, Reply, State).

-compile({inline, [stop/2, stop/3]}).
stop(Reason, State) ->
    {stop, Reason, State}.

stop(Reason, Reply, State) ->
    {stop, Reason, Reply, State}.

