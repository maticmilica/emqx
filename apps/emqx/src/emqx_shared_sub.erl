%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_shared_sub).

-behaviour(gen_server).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

%-import(string,[concat/2]). 

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).

%% APIs
-export([start_link/0]).

-export([
    subscribe/4,
    unsubscribe/4
]).

-export([
    dispatch/3,
    dispatch/4
]).

-export([
    maybe_ack/1,
    maybe_nack_dropped/1,
    nack_no_connection/1,
    is_ack_required/1,
    get_group/1
]).

%% for testing
-ifdef(TEST).
-export([
    subscribers/2,
    strategy/1
]).
-endif.

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).


-export([update_weight/2, update_weightsum/3]).

-export_type([strategy/0]).

-type strategy() ::
    random
    | round_robin
    | sticky
    | local
    %% same as hash_clientid, backward compatible
    | hash
    | hash_clientid
    | hash_topic
    | milica
    | advanced.

-define(SERVER, ?MODULE).
-define(TAB, emqx_shared_subscription).
-define(TAB_WEIGHT, emqx_client_weight).
-define(TAB_WEIGHTSUM, emqx_weightsum).
-define(TAB_POSSIBILITIES, emqx_possibilities).
-define(SHARED_SUBS, emqx_shared_subscriber).
-define(ALIVE_SUBS, emqx_alive_shared_subscribers).
-define(SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS, 5).
-define(IS_LOCAL_PID(Pid), (is_pid(Pid) andalso node(Pid) =:= node())).
-define(ACK, shared_sub_ack).
-define(NACK(Reason), {shared_sub_nack, Reason}).
-define(NO_ACK, no_ack).

-record(state, {pmon}).

-record(emqx_shared_subscription, {group, topic, subpid}).
-record(emqx_client_weight, {subpid, weight}).
-record(emqx_weightsum, {group, topic, weightsum}).
-record(emqx_possibilities, {group, topic, p_array}).

%%--------------------------------------------------------------------
%% Mnesia bootstrap
%%--------------------------------------------------------------------

mnesia(boot) ->
    mria:create_table(?TAB, [
        {type, bag},
        {rlog_shard, ?SHARED_SUB_SHARD},
        {storage, ram_copies},
        {record_name, emqx_shared_subscription},
        {attributes, record_info(fields, emqx_shared_subscription)}
    ]),
    mria:create_table(?TAB_WEIGHT, [
        {type, set},
        {rlog_shard, ?SHARED_SUB_SHARD},
        {storage, ram_copies},
        {record_name, emqx_client_weight},
        {attributes, record_info(fields, emqx_client_weight)}
    ]),
    mria:create_table(?TAB_WEIGHTSUM, [
        {type, set},
        {rlog_shard, ?SHARED_SUB_SHARD},
        {storage, ram_copies},
        {record_name, emqx_weightsum},
        {attributes, record_info(fields, emqx_weightsum)}
    ]),
    mria:create_table(?TAB_POSSIBILITIES, [
        {type, set},
        {rlog_shard, ?SHARED_SUB_SHARD},
        {storage, ram_copies},
        {record_name, emqx_possibilities},
        {attributes, record_info(fields, emqx_possibilities)}
    ]),


    %TabOpts = [public, {read_concurrency, true}, {write_concurrency, true}],
    %ok = emqx_tables:new(?WEIGHTSUM, [set | TabOpts]),
    %ok = emqx_tables:new(?P_ARRAY, [set | TabOpts]),
    
    ok.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link() -> startlink_ret().
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec subscribe(emqx_types:group(), emqx_types:topic(), pid(), emqx_types:weight()) -> ok.
subscribe(Group, Topic, SubPid, Weight) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Group, Topic, SubPid, Weight}).

% petlja koja ide od 1 do NumL i ubacuje element Elem u listu L, vraca L kao povratnu vrednost
loop(L, Elem, NumL) -> 
    loop(L, Elem, NumL, 0).
loop(L, _, [], _) -> 
    L;
loop(L, Elem, [_|T], Acc) ->
    loop(L++[Elem], Elem, T, Acc+1).

% petlja koja ide od 1 do NumL i brise element Elem iz liste L, vraca L kao povratnu vrednost
loop_delete(L, Elem, NumL) -> 
    loop_delete(L, Elem, NumL, 0).
loop_delete(L, _, [], _) -> 
    L;
loop_delete(L, Elem, [_|T], Acc) ->
    NewL = lists:delete(Elem,L),
    loop_delete(NewL, Elem, T, Acc+1).

%unsubscribe(Group, Topic, SubPid) when is_pid(SubPid) ->
%    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid}).
-spec unsubscribe(emqx_types:group(), emqx_types:topic(), pid(), integer) -> ok.
unsubscribe(Group, Topic, SubPid, Weight) when is_pid(SubPid) ->
    gen_server:call(?SERVER, {unsubscribe, Group, Topic, SubPid, Weight}).


record(Group, Topic, SubPid) ->
    #emqx_shared_subscription{group = Group, topic = Topic, subpid = SubPid}.

create_weight_record(SubPid, Weight) ->
    #emqx_client_weight{subpid = SubPid, weight = Weight}.

weightsum_record(Group, Topic, WeightSum) ->
    #emqx_weightsum{group = Group, topic = Topic, weightsum = WeightSum}.

possibilities_record(Group, Topic, Array) ->
    #emqx_possibilities{group = Group, topic = Topic, p_array = Array}.

-spec dispatch(emqx_types:group(), emqx_types:topic(), emqx_types:delivery()) ->
    emqx_types:deliver_result().
dispatch(Group, Topic, Delivery) ->
    dispatch(Group, Topic, Delivery, _FailedSubs = []).

dispatch(Group, Topic, Delivery = #delivery{message = Msg}, FailedSubs) ->
    % Client id ovde je id od onoga ko salje (publisher)
    % u ovu funkciju udje 1 za svaku grupu
    #message{from = ClientId, topic = SourceTopic} = Msg,
    case pick(strategy(Group), ClientId, SourceTopic, Group, Topic, FailedSubs) of
        false ->
            {error, no_subscribers};
        {Type, SubPid} ->
            case do_dispatch(SubPid, Group, Topic, Msg, Type) of
                ok ->
                    {ok, 1};
                {error, _Reason} ->
                    %% Failed to dispatch to this sub, try next.
                    dispatch(Group, Topic, Delivery, [SubPid | FailedSubs])
            end
    end.

-spec strategy(emqx_topic:group()) -> strategy().
strategy(Group) ->
    case emqx:get_config([broker, shared_subscription_group, Group, strategy], undefined) of
        undefined -> emqx:get_config([broker, shared_subscription_strategy]);
        Strategy -> Strategy
    end.

-spec ack_enabled() -> boolean().
ack_enabled() ->
    emqx:get_config([broker, shared_dispatch_ack_enabled]).

do_dispatch(SubPid, _Group, Topic, Msg, _Type) when SubPid =:= self() ->
    %% Deadlock otherwise
    SubPid ! {deliver, Topic, Msg},
    ok;
%% return either 'ok' (when everything is fine) or 'error'
do_dispatch(SubPid, _Group, Topic, #message{qos = ?QOS_0} = Msg, _Type) ->
    %% For QoS 0 message, send it as regular dispatch
    SubPid ! {deliver, Topic, Msg},
    ok;
do_dispatch(SubPid, _Group, Topic, Msg, retry) ->
    %% Retry implies all subscribers nack:ed, send again without ack
    SubPid ! {deliver, Topic, Msg},
    ok;
do_dispatch(SubPid, Group, Topic, Msg, fresh) ->
    case ack_enabled() of
        true ->
            dispatch_with_ack(SubPid, Group, Topic, Msg);
        false ->
            SubPid ! {deliver, Topic, Msg},
            ok
    end.

dispatch_with_ack(SubPid, Group, Topic, Msg) ->
    %% For QoS 1/2 message, expect an ack
    Ref = erlang:monitor(process, SubPid),
    Sender = self(),
    SubPid ! {deliver, Topic, with_group_ack(Msg, Group, Sender, Ref)},
    Timeout =
        case Msg#message.qos of
            ?QOS_1 -> timer:seconds(?SHARED_SUB_QOS1_DISPATCH_TIMEOUT_SECONDS);
            ?QOS_2 -> infinity
        end,
    try
        receive
            {Ref, ?ACK} ->
                ok;
            {Ref, ?NACK(Reason)} ->
                %% the receive session may nack this message when its queue is full
                {error, Reason};
            {'DOWN', Ref, process, SubPid, Reason} ->
                {error, Reason}
        after Timeout ->
            {error, timeout}
        end
    after
        ok = emqx_pmon:demonitor(Ref)
    end.

with_group_ack(Msg, Group, Sender, Ref) ->
    emqx_message:set_headers(#{shared_dispatch_ack => {Group, Sender, Ref}}, Msg).

-spec without_group_ack(emqx_types:message()) -> emqx_types:message().
without_group_ack(Msg) ->
    emqx_message:set_headers(#{shared_dispatch_ack => ?NO_ACK}, Msg).

get_group_ack(Msg) ->
    emqx_message:get_header(shared_dispatch_ack, Msg, ?NO_ACK).

-spec is_ack_required(emqx_types:message()) -> boolean().
is_ack_required(Msg) -> ?NO_ACK =/= get_group_ack(Msg).

-spec get_group(emqx_types:message()) -> {ok, any()} | error.
get_group(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK -> error;
        {Group, _Sender, _Ref} -> {ok, Group}
    end.

%% @doc Negative ack dropped message due to inflight window or message queue being full.
-spec maybe_nack_dropped(emqx_types:message()) -> boolean().
maybe_nack_dropped(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK -> false;
        {_Group, Sender, Ref} -> ok == nack(Sender, Ref, dropped)
    end.

%% @doc Negative ack message due to connection down.
%% Assuming this function is always called when ack is required
%% i.e is_ack_required returned true.
-spec nack_no_connection(emqx_types:message()) -> ok.
nack_no_connection(Msg) ->
    {_Group, Sender, Ref} = get_group_ack(Msg),
    nack(Sender, Ref, no_connection).

-spec nack(pid(), reference(), dropped | no_connection) -> ok.
nack(Sender, Ref, Reason) ->
    Sender ! {Ref, ?NACK(Reason)},
    ok.

-spec maybe_ack(emqx_types:message()) -> emqx_types:message().
maybe_ack(Msg) ->
    case get_group_ack(Msg) of
        ?NO_ACK ->
            Msg;
        {_Group, Sender, Ref} ->
            Sender ! {Ref, ?ACK},
            without_group_ack(Msg)
    end.

pick(sticky, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    Sub0 = erlang:get({shared_sub_sticky, Group, Topic}),
    case is_active_sub(Sub0, FailedSubs) of
        true ->
            %% the old subscriber is still alive
            %% keep using it for sticky strategy
            {fresh, Sub0};
        false ->
            %% randomly pick one for the first message
            {Type, Sub} = do_pick(random, ClientId, SourceTopic, Group, Topic, [Sub0 | FailedSubs]),
            %% stick to whatever pick result
            erlang:put({shared_sub_sticky, Group, Topic}, Sub),
            {Type, Sub}
    end;
pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs).

do_pick(Strategy, ClientId, SourceTopic, Group, Topic, FailedSubs) ->
    All = subscribers(Group, Topic),
    ?SLOG(debug, #{msg => "DO PICK -----------------------", all => All}),
    case All -- FailedSubs of
        [] when All =:= [] ->
            %% Genuinely no subscriber
            false;
        [] ->
            %% All offline? pick one anyway
            {retry, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, All)};
        Subs ->
            %% More than one available
            {fresh, pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs)}
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%OVO JE STARO
% pick_subscriber(_Group, _Topic, _Strategy, _ClientId, _SourceTopic, [Sub]) -> Sub;
% pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs) ->
%     Nth = do_pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, length(Subs)),
%     lists:nth(Nth, Subs).

% do_pick_subscriber(_Group, _Topic, random, _ClientId, _SourceTopic, Count) ->
%     rand:uniform(Count);

% do_pick_subscriber(Group, Topic, hash, ClientId, SourceTopic, Count) ->
%     %% backward compatible
%     do_pick_subscriber(Group, Topic, hash_clientid, ClientId, SourceTopic, Count);

% do_pick_subscriber(_Group, _Topic, hash_clientid, ClientId, _SourceTopic, Count) ->
%     1 + erlang:phash2(ClientId) rem Count;

% do_pick_subscriber(_Group, _Topic, hash_topic, _ClientId, SourceTopic, Count) ->
%     1 + erlang:phash2(SourceTopic) rem Count;

% do_pick_subscriber(Group, Topic, round_robin, _ClientId, _SourceTopic, Count) ->
%     Rem = case erlang:get({shared_sub_round_robin, Group, Topic}) of
%               undefined -> rand:uniform(Count) - 1;
%               N -> (N + 1) rem Count
%           end,
%     _ = erlang:put({shared_sub_round_robin, Group, Topic}, Rem),
%     Rem + 1;
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

pick_subscriber(_Group, _Topic, _Strategy, _ClientId, _SourceTopic, [Sub]) ->
    Sub;

pick_subscriber(Group, Topic, local, ClientId, SourceTopic, Subs) ->
    case lists:filter(fun(Pid) -> erlang:node(Pid) =:= node() end, Subs) of
        [_ | _] = LocalSubs ->
            pick_subscriber(Group, Topic, random, ClientId, SourceTopic, LocalSubs);
        [] ->
            pick_subscriber(Group, Topic, random, ClientId, SourceTopic, Subs)
    end;

pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs) ->
    %Nth = do_pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs),
    %lists:nth(Nth, Subs).
    do_pick_subscriber(Group, Topic, Strategy, ClientId, SourceTopic, Subs).

do_pick_subscriber(_Group, _Topic, random, _ClientId, _SourceTopic, Subs) ->
    Count = length(Subs),
    Nth = rand:uniform(Count),
    lists:nth(Nth, Subs);


do_pick_subscriber(Group, Topic, hash, ClientId, SourceTopic, Subs) ->
    %% backward compatible
    Nth = do_pick_subscriber(Group, Topic, hash_clientid, ClientId, SourceTopic, Subs),
    lists:nth(Nth, Subs);


do_pick_subscriber(_Group, _Topic, hash_clientid, ClientId, _SourceTopic, Subs) ->
    Count = length(Subs),
    Nth = 1 + erlang:phash2(ClientId) rem Count,
    lists:nth(Nth, Subs);


do_pick_subscriber(_Group, _Topic, hash_topic, _ClientId, SourceTopic, Subs) ->
    Count = length(Subs),
    Nth = 1 + erlang:phash2(SourceTopic) rem Count,
    lists:nth(Nth, Subs);


do_pick_subscriber(Group, Topic, round_robin, _ClientId, _SourceTopic, Subs) ->
    Count = length(Subs),
    Rem = case erlang:get({shared_sub_round_robin, Group, Topic}) of
              undefined -> rand:uniform(Count) - 1;
              N -> (N + 1) rem Count
          end,
    _ = erlang:put({shared_sub_round_robin, Group, Topic}, Rem),
    Nth = Rem + 1,
    lists:nth(Nth, Subs);

% Finds client by priority: 
% the biggest priority has weight
% if there is more than one client with the same priority, messages will be sent to them in rr order
do_pick_subscriber(_Group, _Topic, milica, _ClientId, _SourceTopic, Subs) ->
    
    Init = #{weight => 11, index => 0, indexlist => []},
    
        ?SLOG(debug, #{msg => "MILICAAAAAAAAA ----------------------", sve => ets:match_object(?TAB_WEIGHT, {'$0', '$1', '$2'})}),


    Acc = lists:foldl(fun(SPid, IterationMap) -> 
        %Weight = emqx_broker:get_weight(SPid),             OVAKO U LOKALU, KAD SE SALJE NA CONNECT

        Weight = get_weight(SPid),
        MinElem = maps:get(weight, IterationMap),
        CurrentIndex = maps:get(index, IterationMap),
        MinIndexList = maps:get(indexlist, IterationMap),     

        if 
            Weight < MinElem ->
                #{weight => Weight, index => CurrentIndex + 1, indexlist => [CurrentIndex+1]};
            Weight == MinElem ->
                #{weight => Weight, index => CurrentIndex + 1, indexlist => MinIndexList ++ [CurrentIndex+1]};
            true ->
                #{weight => MinElem, index => CurrentIndex + 1, indexlist => MinIndexList}
        end

    end, Init, Subs),

    % Acc is map with indexlist
    % indeksi klijenata koji imaju min weight je u indexlist, ona se prosledjuje u rr
    SubPidsMin = maps:get(indexlist, Acc),
    do_pick_subscriber(_Group, _Topic, round_robin, _ClientId, _SourceTopic, SubPidsMin);

do_pick_subscriber(Group, Topic, advanced, _ClientId, _SourceTopic, _Subs) ->
    %Name = concat(binary_to_list(Group), binary_to_list(Topic)),   
    %[{_Record, _G, _T, Sum}] = ets:match_object(?TAB_WEIGHTSUM, {Group, Topic, '$2'}),

    [{_Record, _G, _T, Sum}] = mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'}),
    % Sum je ukupna suma tezina klijenata prijavljenih na grupu Group i topic Topic
    
    X = rand:uniform(Sum),
    %[{_G, _T, ListClients}] = ets:match_object(?P_ARRAY, {Group, Topic, '$2'}),
    [{_R, _G1, _T1, ListClients}] = mnesia:dirty_match_object(#emqx_possibilities{group = Group, topic = Topic, _ = '_'}),
    Cli = lists:nth(X, ListClients),
    
    ?SLOG(debug, #{msg => "ADVANCED WEIGHT SUM----------------------",  weightsum => Sum, cliiii => Cli, listtt => ListClients}),
    
    Cli.


subscribers(Group, Topic) ->
    ets:select(?TAB, [{{emqx_shared_subscription, Group, Topic, '$1'}, [], ['$1']}]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    ok = mria:wait_for_tables([?TAB, ?TAB_WEIGHT]),
    {ok, _} = mnesia:subscribe({table, ?TAB, simple}),
    {ok, _} = mnesia:subscribe({table, ?TAB_WEIGHT, simple}),
    %{ok, _} = mnesia:subscribe({table, ?TAB_WEIGHTSUM, simple}),
    %{ok, _} = mnesia:subscribe({table, ?TAB_POSSIBILITIES, simple}),
    {atomic, PMon} = mria:transaction(?SHARED_SUB_SHARD, fun init_monitors/0),
    ok = emqx_tables:new(?SHARED_SUBS, [protected, bag]),
    ok = emqx_tables:new(?ALIVE_SUBS, [protected, set, {read_concurrency, true}]),
    {ok, update_stats(#state{pmon = PMon})}.

init_monitors() ->
    mnesia:foldl(
        fun(#emqx_shared_subscription{subpid = SubPid}, Mon) ->
            emqx_pmon:monitor(SubPid, Mon)
        end,
        emqx_pmon:new(),
        ?TAB
    ).

handle_call({subscribe, Group, Topic, SubPid, Weight}, _From, State = #state{pmon = PMon}) ->
    mria:dirty_write(?TAB, record(Group, Topic, SubPid)),
    update_weight(SubPid, Weight),
    calculate_weight_sum(Group, Topic, Weight),
    generate_possibilities_array(Group, Topic, SubPid, Weight),
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true -> ok;
        false -> ok = emqx_router:do_add_route(Topic, {Group, node()})
    end,
    ok = maybe_insert_alive_tab(SubPid),
    true = ets:insert(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    {reply, ok, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

handle_call({unsubscribe, Group, Topic, SubPid, Weight}, _From, State) ->

    mria:dirty_delete_object(?TAB, record(Group, Topic, SubPid)),
    mria:dirty_delete_object(?TAB_WEIGHT, create_weight_record(SubPid, Weight)),
    decrease_weightsum(Group, Topic, Weight),
    remove_from_possibilities_array(Group, Topic, SubPid, Weight),

    true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
    delete_route_if_needed({Group, Topic}),
    {reply, ok, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", req => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_cast", req => Msg}),
    {noreply, State}.


% da li su ove 2 funkcije potrebne? Mozda zbog clustera kao event da je novi dodat?
%%%%%%%%%%%%%%%%%%%
handle_info({mnesia_table_event, {write, NewRecord, _}}, _State = #state{pmon = _PMon}) when is_record(NewRecord, emqx_possibilities) ->
    ?SLOG(debug, #{msg => "mnesia write eventttt posibbbb ----------------------", record => NewRecord});
    %#emqx_possibilities{p_array = Array} = NewRecord,
    %{noreply, update_stats(State#state{pmon = emqx_pmon:monitor(Array, PMon)})};

handle_info({mnesia_table_event, {write, NewRecord, _}}, _State = #state{pmon = _PMon}) when is_record(NewRecord, emqx_weightsum) ->
    ?SLOG(debug, #{msg => "mnesia write eventttttt summmmm ----------------------", record => NewRecord});
    %#emqx_weightsum{weightsum = WeightSum} = NewRecord,
    %{noreply, update_stats(State#state{pmon = emqx_pmon:monitor(WeightSum, PMon)})};

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_info({mnesia_table_event, {write, NewRecord, _}}, State = #state{pmon = PMon}) when is_record(NewRecord, emqx_client_weight) ->
    #emqx_client_weight{subpid = SubPid} = NewRecord,
    {noreply, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};

handle_info({mnesia_table_event, {write, NewRecord, _}}, State = #state{pmon = PMon}) ->
    #emqx_shared_subscription{subpid = SubPid} = NewRecord,

    {noreply, update_stats(State#state{pmon = emqx_pmon:monitor(SubPid, PMon)})};
%% The subscriber may have subscribed multiple topics, so we need to keep monitoring the PID until
%% it `unsubscribed` the last topic.
%% The trick is we don't demonitor the subscriber here, and (after a long time) it will eventually
%% be disconnected.
% handle_info({mnesia_table_event, {delete_object, OldRecord, _}}, State = #state{pmon = PMon}) ->
%     #emqx_shared_subscription{subpid = SubPid} = OldRecord,
%     {noreply, update_stats(State#state{pmon = emqx_pmon:demonitor(SubPid, PMon)})};

handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, State};
handle_info({'DOWN', _MRef, process, SubPid, Reason}, State = #state{pmon = PMon}) ->
    ?SLOG(info, #{msg => "shared_subscriber_down", sub_pid => SubPid, reason => Reason}),
    cleanup_down(SubPid),
    {noreply, update_stats(State#state{pmon = emqx_pmon:erase(SubPid, PMon)})};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, ?TAB, simple}),
    mnesia:unsubscribe({table, ?TAB_WEIGHT, simple}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% keep track of alive remote pids
maybe_insert_alive_tab(Pid) when ?IS_LOCAL_PID(Pid) -> ok;
maybe_insert_alive_tab(Pid) when is_pid(Pid) ->
    ets:insert(?ALIVE_SUBS, {Pid}),
    ok.

cleanup_down(SubPid) ->
    ?IS_LOCAL_PID(SubPid) orelse ets:delete(?ALIVE_SUBS, SubPid),

    % get Group, Topic and Weight from SubPid
    [{_, G, T, _}] = mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid}),
    [{_, _P, W}] = mnesia:dirty_match_object(#emqx_client_weight{_ = '_', subpid = SubPid}),
    
    % remove from array
    remove_from_possibilities_array(G, T, SubPid, W),

    % decrease weightsum
    decrease_weightsum(G, T, W),

    ?SLOG(debug, #{msg => "------------------- CLEAN UPPPPPPPPPPP", 
        nizzzzzzzz => mnesia:dirty_match_object(#emqx_possibilities{group = G, topic = T, _ = '_'}),
        summmm => mnesia:dirty_match_object(#emqx_client_weight{_ = '_', subpid = SubPid})
    }),

    lists:foreach(
        fun(Record = #emqx_shared_subscription{topic = Topic, group = Group}) ->
            ok = mria:dirty_delete_object(?TAB, Record),
            true = ets:delete_object(?SHARED_SUBS, {{Group, Topic}, SubPid}),
            delete_route_if_needed({Group, Topic})
        end,
        mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = SubPid})
    ),
    lists:foreach(
        fun(Record = #emqx_client_weight{subpid = _SubPid, weight = _Weight}) ->
            ok = mria:dirty_delete_object(?TAB_WEIGHT, Record)
        end,
        mnesia:dirty_match_object(#emqx_client_weight{_ = '_', subpid = SubPid})
    )
    .

update_stats(State) ->
    emqx_stats:setstat(
        'subscriptions.shared.count',
        'subscriptions.shared.max',
        ets:info(?TAB, size)
    ),
    State.

%% Return 'true' if the subscriber process is alive AND not in the failed list
is_active_sub(Pid, FailedSubs) ->
    is_alive_sub(Pid) andalso not lists:member(Pid, FailedSubs).

%% erlang:is_process_alive/1 does not work with remote pid.
is_alive_sub(Pid) when ?IS_LOCAL_PID(Pid) ->
    erlang:is_process_alive(Pid);
is_alive_sub(Pid) ->
    [] =/= ets:lookup(?ALIVE_SUBS, Pid).

delete_route_if_needed({Group, Topic}) ->
    case ets:member(?SHARED_SUBS, {Group, Topic}) of
        true -> ok;
        false -> ok = emqx_router:do_delete_route(Topic, {Group, node()})
    end.

get_weight(SubPid) ->
    WeightList = ets:lookup_element(?TAB_WEIGHT, SubPid, 3),
    lists:nth(1, WeightList).

update_weight(SubPid, NewWeight) ->
    OldRecordsList = mnesia:dirty_match_object(#emqx_client_weight{_ = '_', subpid = SubPid}),
    if 
        length(OldRecordsList) /= 0 ->
            Record = lists:nth(1, OldRecordsList),
            ok = mria:dirty_delete_object(?TAB_WEIGHT, Record);
        true ->
            ok
    end,

    NewRecord = create_weight_record(SubPid, NewWeight),
    mria:dirty_write(?TAB_WEIGHT, NewRecord).

calculate_weight_sum(Group, Topic, Weight) ->
    case mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'}) of
        [] -> 
            mria:dirty_write(?TAB_WEIGHTSUM, weightsum_record(Group, Topic, Weight));
        [{_, G, T, Sum}] -> 
           mria:dirty_write(?TAB_WEIGHTSUM, weightsum_record(G, T, Sum+Weight))
    end,

    ?SLOG(debug, #{msg => "------------------- WEIGHT SUM-----------------------", 
        xxxx => mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'})})
    .
    
decrease_weightsum(Group, Topic, Weight) ->
    [{_, G, T, Sum}] = mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'}),

    mria:dirty_write(?TAB_WEIGHTSUM, weightsum_record(G, T, Sum-Weight))
   
    %?SLOG(debug, #{msg => "------------------- WEIGHT SUM-----------------------", 
    %    xxxx => mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'})})
    .

update_weightsum(Pid, NewWeight, OldWeight) ->
    % find Group and Topic from Pid
    [{_, Group, Topic, _}] = mnesia:dirty_match_object(#emqx_shared_subscription{_ = '_', subpid = Pid}),
    Diff = NewWeight - OldWeight,
    % Find Sum and update it with diff
    [{_, G, T, Sum}] = mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'}),
    mria:dirty_write(?TAB_WEIGHTSUM, weightsum_record(G, T, Diff+Sum)),

    update_possibilities(G, T, Pid, OldWeight, NewWeight),
    
    ?SLOG(debug, #{msg => "------------------- UPDATE WEIGHT SUM 222222222222222222-----------------------", 
        aaa => mnesia:dirty_match_object(#emqx_weightsum{group = Group, topic = Topic, _ = '_'})})
    .

generate_possibilities_array(Group, Topic, SubPid, Weight) ->
    
    case mnesia:dirty_match_object(#emqx_possibilities{group = Group, topic = Topic, _ = '_'}) of
        [] -> 
            X = lists:seq(1,Weight),
            NewL = loop([], SubPid, X),
            mria:dirty_write(?TAB_POSSIBILITIES, possibilities_record(Group, Topic, NewL));
       [{_, G1, T1, L}] -> 
            X1 = lists:seq(1, Weight),
            NewL1 = loop(L, SubPid, X1),
            mria:dirty_write(?TAB_POSSIBILITIES, possibilities_record(G1, T1, NewL1))
    end,

    ?SLOG(debug, #{msg => "------------------- NIYYYYY -----------------------", 
        xxxx => mnesia:dirty_match_object(#emqx_possibilities{group = Group, topic = Topic, _ = '_'})})
    
    .

remove_from_possibilities_array(Group, Topic, SubPid, Weight) ->
    [{_, G, T, List}] = mnesia:dirty_match_object(#emqx_possibilities{group = Group, topic = Topic, _ = '_'}),
    NoElems = lists:seq(1, Weight),
    NewList = loop_delete(List, SubPid, NoElems),
    mria:dirty_write(?TAB_POSSIBILITIES, possibilities_record(G, T, NewList)).

update_possibilities(Gruop, Topic, SubPid, OldWeight, NewWeight) ->
    remove_from_possibilities_array(Gruop, Topic, SubPid, OldWeight),
    generate_possibilities_array(Gruop, Topic, SubPid, NewWeight).


