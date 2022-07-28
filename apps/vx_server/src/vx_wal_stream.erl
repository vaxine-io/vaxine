%% @doc Module that encapsulates logic for WAL replication.
%% Current known limitations for this module:
%% - no support for starting replication from arbitrary position
%% - no streaming support for replication of multiple partitions

-module(vx_wal_stream).
-behaviour(gen_statem).

-export([ start_link/1,
          start_replication/3,
          stop_replication/1,
          notify_commit/2
        ]).

-export([ init/1,
          init_stream/3,
          await_data/3,
          callback_mode/0,
          terminate/3,
          code_change/4
        ]).

%% -include_lib("kernel/src/disk_log.hrl").
-record(continuation,         %% Chunk continuation.
        {pid = self() :: pid(),
         pos          :: non_neg_integer() | {integer(), non_neg_integer()},
         b            :: binary() | [] | pos_integer()
        }).
-include_lib("stdlib/include/ms_transform.hrl").

-include_lib("antidote/include/antidote.hrl").
-include("vx_wal_stream.hrl").

-define(POLL_RETRY_MIN, 10).
-define(POLL_RETRY_MAX, timer:seconds(10)).

-define(READ_TXN(TxId, DcId, TxCommitTime, TxOffset, Term),
        {TxId, DcId, TxCommitTime, TxOffset, Term}
       ).

-record(data, {client :: pid() | undefined,
               mon_ref:: reference() | undefined,
               file_status :: more_data | eof,
               file_desc :: file:fd() | undefined,
               file_name :: file:filename_all(),
               file_buff = [] :: term(),
               file_pos = 0 :: non_neg_integer(),
               txns_buffer :: txns_noncomitted_map(),
               %% Last read TxId. May be either comitted or aborted
               last_read_txid :: antidote:txid() | undefined,
               %% Last txid we have been notified about
               last_notif_txid :: antidote:txid() | undefined,

               %% NOTE: Keeps last tx per dc, so that we can filter tx
               %% that we have already send previously. No two transactions
               %% from the same DC can have a same clock_time on a partition
               last_send_map = #{} ::
                 #{ antidote:dcid() => antidote:clock_time() },
               last_send_offset = 0 :: non_neg_integer(),

               %% Retry for file polling
               file_poll_tref :: reference() | undefined,
               file_poll_backoff :: backoff:backoff(),
               partition :: antidote:partition_id(),

               %% Buffer with materialized transasction that have not
               %% been sent
               to_send = [],
               %% Retry backoff for tcp port send
               port_retry_tref :: reference() | undefined,
               port_retry_backoff :: backoff:backoff(),
               port :: port() | undefined
              }).

-type wal_offset() :: term().

-record(commit, { partition :: antidote:partition_id(),
                  txid :: antidote:txid(),
                  snapshot :: snapshot_time()
                }).

-export_type([wal_offset/0]).

-spec start_link(list()) -> {ok, pid()} | ignore | {error, term()}.
start_link(Args) ->
    gen_statem:start_link(?MODULE, Args, []).

%% @doc Starts replication, Port is expected to be a tcp port, opened by
%% vx_wal_tcp_worker.
-spec start_replication(pid(), port(), list()) ->
          {ok, pid()} | {error, term()}.
start_replication(Pid, Port, DiskLogOpts) ->
    gen_statem:call(Pid, {start_replication, Port, DiskLogOpts}, infinity).

%% @doc Ask vx_wal_stream to stop replicating of the data. At the moment it's not
%% possible to pause and resume replication. Process does not expect any further
%% calls after call ( the only thing could be done here to terminate the process
%% ).
-spec stop_replication(pid()) -> ok | {error, term()}.
stop_replication(Pid) ->
    gen_statem:call(Pid, {stop_replication}).

%% @doc Callback for notification of vx_wal_stream about committed transactions.
%% Current logic would only notify the process with cast message if the status
%% is set to ready.
-spec notify_commit(term(), pid()) -> ok.
notify_commit({commit, Partition, TxId, CommitTime, SnapshotTime} = V, Pid) ->
    try
        logger:info("commit notification ~p~n~p~n", [TxId, V]),
        true = ets:update_element(wal_replication_status, {Partition, Pid},
                           { #wal_replication_status.txdata,
                             {TxId, CommitTime, SnapshotTime}
                           }),
        RFun = ets:fun2ms(
                 fun(#wal_replication_status{key = {Partition0, Pid0},
                                             notification = ready
                                            } = W)
                    when Partition0 == Partition,
                         Pid0 == Pid->
                         W#wal_replication_status{notification = sent}
                 end),

        case ets:select_replace(wal_replication_status, RFun) of
            0 -> ok;
            1 ->
                Pid ! #commit{ partition = Partition, txid = TxId,
                               snapshot = SnapshotTime
                             },
                ok
        end
    catch T:E:_ ->
            logger:error("Notification failed ~p~n", [{T, E}]),
            ok
    end.

init_notification_slot(Partition) ->
    logger:info("Init notification slot for partition ~p~n", [Partition]),
    ets:insert(wal_replication_status,
               #wal_replication_status{key = {Partition, self()},
                                       notification = ready
                                      }).

-spec mark_as_ready_for_notification(antidote:partition_id()) -> ok.
mark_as_ready_for_notification(Partition) ->
    logger:debug("Mark as ready to continue wal streaming: ~p~n", [Partition]),
    true = ets:update_element(wal_replication_status, {Partition, self()},
                              [{#wal_replication_status.notification, ready}]),
    ok.

callback_mode() ->
    state_functions.

init(_Args) ->
    %% FIXME: Only work with a single partition at the moment
    %% FIXME: Move this initialization back to start_replication
    [Partition | _] = dc_utilities:get_all_partitions(),
    %% We would like to know where the file is located
    InfoList = disk_log:info(log_path(Partition)),
    LogFile = proplists:get_value(file, InfoList),
    halt    = proplists:get_value(type, InfoList), %% Only handle halt type of the logs

    FilePollBackoff0 = backoff:init(?POLL_RETRY_MIN, ?POLL_RETRY_MAX,
                                    self(), file_poll_retry),
    FilePollBackoff1 = backoff:type(FilePollBackoff0, jitter),

    PortSendBackoff0 = backoff:init(?POLL_RETRY_MIN, ?POLL_RETRY_MAX,
                                    self(), port_send_retry),
    PortSendBackoff1 = backoff:type(PortSendBackoff0, jitter),

    ok = vx_wal_stream_server:register(),
    {ok, init_stream, #data{file_name = LogFile,
                            txns_buffer = #{},
                            partition = Partition,
                            file_poll_backoff = FilePollBackoff1,
                            port_retry_backoff = PortSendBackoff1,
                            file_status = more_data
                           }}.

%% Copied from logging_vnode and simplified for our case
log_path(Partition) ->
    LogFile = integer_to_list(Partition),
    {ok, DataDir} = application:get_env(antidote, data_dir),
    LogId = LogFile ++ "--" ++ LogFile,
    filename:join(DataDir, LogId).

init_stream({call, {Sender, _} = F}, {start_replication, Port, Opts}, Data) ->
    %% FIXME: We support only single partition for now
    WalOffset = case proplists:get_value(offset, Opts, none) of
                  none -> 0;
                  Offset -> Offset
              end,
    {ok, FD} = open_log(Data#data.file_name),
    MonRef = erlang:monitor(process, Sender),

    true = init_notification_slot(Data#data.partition),
    ok = logging_notification_server:add_handler(
           ?MODULE, notify_commit, [self()]),
    {_, Backoff} = backoff:succeed(Data#data.file_poll_backoff),

    {next_state, await_data, Data#data{client = Sender,
                                       mon_ref = MonRef,
                                       file_pos = WalOffset,
                                       file_buff = [],
                                       file_desc = FD,
                                       file_poll_backoff = Backoff,
                                       port = Port
                                      },
     [{state_timeout, 0, {timeout, undefined, file_poll_retry} },
      {reply, F, {ok, self()}}]};

init_stream(info, {gen_event_EXIT, _Handler, _Reason}, Data) ->
    {keep_state, Data}.

await_data(_, #commit{txid = TxId}, #data{file_poll_tref = undefined} = Data) ->
    continue_wal_reading(Data#data{last_notif_txid = TxId});

await_data(_, #commit{}, Data) ->
    {keep_state, Data};

await_data(_, {timeout, TRef, file_poll_retry}, Data = #data{file_poll_tref = TRef}) ->
    logger:debug("file poll retry: ~p~n", [Data#data.file_poll_backoff]),
    continue_wal_reading(Data#data{file_poll_tref = undefined});

await_data(_, {timeout, TRef, port_send_retry}, Data = #data{port_retry_tref = TRef}) ->
    logger:debug("tpc port retry: ~p~n", [Data#data.port_retry_backoff]),
    continue_send(Data#data{port_retry_tref = undefined});

await_data({call, Sender}, {stop_replication}, Data) ->
    _ = file:close(Data#data.file_desc),
    ok = logging_notification_server:delete_handler([]),
    erlang:demonitor(Data#data.mon_ref),
    {_, FBackoff} = backoff:succeed(Data#data.file_poll_backoff),
    {_, PBackoff} = backoff:succeed(Data#data.port_retry_backoff),
    _ = case Data#data.file_poll_tref of
            undefined -> ok;
            TRef0 -> erlang:cancel_timer(TRef0)
        end,
    _ = case Data#data.port_retry_tref of
            undefined -> ok;
            TRef1 -> erlang:cancel_timer(TRef1)
        end,
    {next_state, init_stream, #data{file_name = Data#data.file_name,
                                    file_status = more_data,
                                    txns_buffer = #{},
                                    partition = Data#data.partition,
                                    file_poll_backoff = FBackoff,
                                    port_retry_backoff = PBackoff
                                   },
     [{reply, Sender, ok}]
    };

await_data({call, Sender}, Msg, Data) ->
    %% FIXME: handle {stop_replication} message here
    logger:info("Ignored message: ~p~n", [Msg]),
    {keep_state, Data, [{reply, Sender, {error, unhandled_msg}}] };

await_data(info, {gen_event_EXIT, _Handler, _Reason}, _Data) ->
    %% FIXME: probably safer just to restart the process
    ok = logging_notification_server:add_handler(
           ?MODULE, notify_commit, [self()]),
    keep_state_and_data;

%% We receive inet_reply messages due to port_command/3 nosuspend
%% call in vx_wal_tcp_worker:send/2
await_data(info, {inet_reply, _Sock, ok}, _Data) ->
    keep_state_and_data;

await_data(info, {inet_reply, _Sock, {error, Reason}}, Data) ->
    logger:error("socket error: ~p~n", [Reason]),
    {stop, {shutdown, Reason}, Data};

await_data(_, Msg, _Data) ->
    %% FIXME: handle {stop_replication} message here
    logger:info("Ignored message wal streamer: ~p~n", [Msg]),
    keep_state_and_data.

%%------------------------------------------------------------------------------

continue_send(#data{} = Data) ->
    case notify_client(Data#data.to_send, Data) of
        {ok, Data1} when Data1#data.file_status == more_data ->
            continue_wal_reading(Data1);
        {retry, Data1} ->
            {keep_state, Data1};
        {error, _Reason} = Error ->
            {stop, Error}
    end.

continue_wal_reading(#data{partition = Partition} = Data) ->
    logger:info("Continue wal streaming for client ~p on partition ~p"
                " at position ~p~n",
                [Data#data.client, Data#data.partition, Data#data.file_pos]),

    case read_ops_from_log(Data) of
        {ok, Data1} when Data1#data.file_status == more_data ->
            {next_state, await_data, Data1};
        {ok, Data1} when Data1#data.file_status == eof ->
            ok = mark_as_ready_for_notification(Partition),
            case fetch_latest_position(Partition) of
                undefined ->
                    {next_state, await_data, Data1};
                {TxId, _, _} when TxId == Data1#data.last_read_txid ->
                    %% All the data is read, no need to set the timer
                    {next_state, await_data, Data1#data{last_notif_txid = TxId}};
                {TxId, _, _} when TxId == Data1#data.last_notif_txid ->
                    %% No matter whether or not we read something new,
                    %% we didn't read TxId transaction, so set the poll timer
                    %% based on backoff value
                    {next_state, await_data, set_file_poll_timer(Data1)};
                {TxId, _, _} ->
                    %% New TxId since we last checked. For now I would like
                    %% to keep it as a separate case here.
                    {next_state, await_data,
                     set_file_poll_timer(Data1#data{last_notif_txid = TxId})}
            end;
        {error, _} = Error ->
            {stop, Error}
    end.

set_file_poll_timer(Data) ->
    Data#data{file_poll_tref = backoff:fire(Data#data.file_poll_backoff)}.

set_port_send_timer(Data) ->
    Data#data{port_retry_tref = backoff:fire(Data#data.port_retry_backoff)}.


-spec fetch_latest_position(antidote:partition_id()) -> undefined |
          {antidote:txid(), antidote:clock_time(), antidote:snapshot_time()}.
fetch_latest_position(Partition) ->
    ets:lookup_element(wal_replication_status,
                       {Partition, self()}, #wal_replication_status.txdata).

materialize(Key, Type, ST, TxId) ->
    {Partition, _} = log_utilities:get_key_partition(Key),
    %% FIXME: Yeah, we do not expect this to fail for now
    {ok, Snapshot} =
        materializer_vnode:read(
          Key, Type, ST, TxId, _PropertyList = [], Partition),
    Snapshot.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% only initial position is supported at the moment.
open_log(LogFile) ->
    {ok, FD} = file:open(LogFile, [raw, binary, read]),
    {ok, _Head} = file:read(FD, _Header = 8),
    {ok, FD}.

read_ops_from_log(Data) ->
    case read_ops_from_log(Data, 0) of
        {ok, 0, Data1} ->
            {_, Backoff} = backoff:fail(Data1#data.file_poll_backoff),
            {ok, Data1#data{file_poll_backoff = Backoff}};
        {ok, _, Data1} ->
            {_, Backoff} = backoff:succeed(Data1#data.file_poll_backoff),
            {ok, Data1#data{file_poll_backoff = Backoff}};
        {error, _} = Error ->
            Error
    end.


%% 1. First of all figure out how do we get offset per transaction.
%% (offset where trans starts).

%% Offset should correspond to the earliest transaction across all tx that are in
%% buffer.

%% 2. For every source DC - keep the last transaction that have been send.
%% This to be used, to skip some of the transaction that go from offset.

%% Maybe this is not even required, just keep the last transaction that you have send
%% before that, given that we intend to have deterministic order anyway? Maybe not,
%% as there might be several transactions which intersect with the one that we are sending
%% but for one trans we have already send, while other we have not.
read_ops_from_log(#data{txns_buffer = TxnBuff,
                        file_pos = FPos,
                        file_buff = FBuff,
                        file_desc = Fd
                       } = Data, N) ->
    case read_ops_from_log(Fd, Data#data.file_name, FPos, FBuff, TxnBuff) of
        %% IF we received eof here, that means we haven't read anything from the
        %% log during this call
        {eof, LogPosition, {NonComittedMap, []}} ->
            %% FIXME: What is the position here?
            {FPos1, FBuff1} = LogPosition,
            {ok, N, Data#data{txns_buffer = NonComittedMap,
                              file_pos = FPos1,
                              file_buff = FBuff1,
                              file_status = eof
                             }};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, {NonComittedMap, ComittedData}} ->
            {FPos1, FBuff1} = LogPosition,
            case
                notify_client(ComittedData, Data#data{file_pos = FPos1,
                                                      file_buff = FBuff1,
                                                      txns_buffer = NonComittedMap,
                                                      file_status = more_data
                                                     })
            of
                {ok, Data1} ->
                    read_ops_from_log(Data1, N+1);
                {retry, Data1} ->
                    {ok, N + 1, Data1};
                {error, _} = Error ->
                    Error
            end
    end.

-type txns_noncomitted_map() :: #{antidote:txid() =>
                                      { file_offset(), [any_log_payload()]}
                                 }.
-type txns_comitted() :: [ { antidote:txid(),
                             antidote:dcid(),
                             antidote:clock_time(),
                             file_offset(),
                             term() } ].

-spec read_ops_from_log(file:fd(), file:filename_all(), non_neg_integer(), term(),
                        txns_noncomitted_map()) ->
          {error, term()} |
          {ok | eof, log_position(),
           { txns_noncomitted_map(), txns_comitted() }
          }.
read_ops_from_log(Fd, FileName, FPos, FBuffer, RemainingOps) ->
    case read_chunk(Fd, FileName, FPos, FBuffer, 100) of
        {eof, LogPosition, []} ->
            {eof, LogPosition, {RemainingOps, []}};
        {error, _} = Error ->
            Error;
        {ok, LogPosition, NewTerms}->
            TxOffset = generate_pos_in_log({FPos, FBuffer}),
            {ok, LogPosition, process_txns(NewTerms, RemainingOps, [], TxOffset)}
    end.

%% If we have emitted new terms -> we can consider offset to be Pos - size(Binary) - headersz
%% If we have not emitted new terms -> we do not update offset, as we expect that the
%% chunk was larger then the MAX_CHUNK_SIXE in disk_log
%% If eof we simple retry with the buffe and last settings
generate_pos_in_log({FPos, FBuf}) ->
    case FBuf of
        [] -> FPos;
        B  -> FPos - byte_size(B) - 8 %% HEADERSZ from disk_log
    end.

-type log_position() :: {non_neg_integer(), Buffer :: term()}.
-spec read_chunk(file:fd(), file:filename_all(), non_neg_integer(), term(), non_neg_integer()) ->
          {ok, log_position(), [ term() ]} |
          {error, term()} |
          {eof, log_position(), [ term() ]}.
read_chunk(Fd, FileName, FPos, FBuff, Amount) ->
    R = disk_log_1:chunk_read_only(Fd, FileName, FPos, FBuff, Amount),
    %% Create terms from the binaries returned from chunk_read_only/5.
    %% 'foo' will do here since Log is not used in read-only mode.
    case disk_log:ichunk_end(R, _Log = foo) of
        {#continuation{pos = FPos1, b = Buffer1}, Terms}
          when FPos == FPos1 ->
            %% The same block but different term position
            {ok, {FPos1, Buffer1}, Terms};
        {#continuation{pos = FPos1, b = Buffer1}, Terms} ->
            {ok, {FPos1, Buffer1}, Terms};
        {error, _} = Error ->
            Error;
        eof ->
            %% That's ok, just need to keep previous position
            {eof, {FPos, FBuff}, []}
    end.

-type file_offset() :: non_neg_integer().

-spec process_txns([{term(), #log_record{}}], txns_noncomitted_map(), txns_comitted(),
                   file_offset()) ->
          {txns_noncomitted_map(), txns_comitted()}.
process_txns([], RemainingOps, FinalizedTxns, _) ->
    {RemainingOps, preprocess_comitted(lists:reverse(FinalizedTxns))};
process_txns([{_, LogRecord} | Rest], RemainingOps, FinalizedTxns0, TxOffset) ->
    #log_record{log_operation = LogOperation} = log_utilities:check_log_record_version(LogRecord),

    {RemainingOps1, FinalizedTxns1} =
        process_op(LogOperation, RemainingOps, FinalizedTxns0, TxOffset),
    process_txns(Rest, RemainingOps1, FinalizedTxns1, TxOffset).

process_op(#log_operation{op_type = OpType, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, TxOffset)
  when OpType == update orelse
       OpType == update_start ->
    {Key, Type, Op} = { Payload#update_log_payload.key,
                        Payload#update_log_payload.type,
                        Payload#update_log_payload.op
                  },
    RemainingOps1 =
        maps:update_with(TxId, fun({Offset, Ops}) ->
                                       {Offset, [{Key, Type, Op} | Ops]}
                               end,
                         {TxOffset, [{Key, Type, Op}]}, RemainingOps),
    {RemainingOps1, FinalizedTxns};
process_op(#log_operation{op_type = prepare}, RemainingOps, FinalizedTxns, _) ->
    {RemainingOps, FinalizedTxns};
process_op(#log_operation{op_type = abort, tx_id = TxId}, RemainingOps, FinalizedTxns, _) ->
    RemainingOps1 = maps:remove(TxId, RemainingOps),
    %% NOTE: We still want to know about this transaction to not loose
    %% track of last transaction id.
    {RemainingOps1, [prepare_txn_operations(TxId, aborted) | FinalizedTxns]};
process_op(#log_operation{op_type = commit, tx_id = TxId, log_payload = Payload},
           RemainingOps, FinalizedTxns, _) ->
    #commit_log_payload{commit_time = {DcId, TxCommitTime},
                        snapshot_time = ST
                       } = Payload,
    TxST = vectorclock:set(DcId, TxCommitTime, ST),

    case maps:take(TxId, RemainingOps) of
        {{TxOffset, TxOpsList}, RemainingOps1} ->
            %% Sort TxOpsList according to file order
            {RemainingOps1,
             [prepare_txn_operations(TxId, TxST, DcId, TxCommitTime, TxOffset,
                                     lists:reverse(TxOpsList))
             | FinalizedTxns]};
        error ->
            logger:warning("Empty transaction: ~p~n", [TxId]),
            {RemainingOps, FinalizedTxns}
    end.

prepare_txn_operations(TxId, TxST, DcId, TxCommitTime, TxOffset, TxOpsList0) ->
    TxOpsDict = lists:foldl(fun({Key, Type, Op}, Acc) ->
                                  dict:append({Key, Type}, Op, Acc)
                              end, dict:new(), TxOpsList0),
    TxKeys = sets:to_list(
               sets:from_list(
                 lists:map(fun({Key, Type, _Op}) -> {Key, Type} end, TxOpsList0))),
    TxMaterializedKeysWithOps =
        lists:map(fun({Key, Type}) ->
                          {Key, Type, materialize(Key, Type, TxST, TxId), dict:fetch({Key, Type}, TxOpsDict)}
                  end, TxKeys),
    logger:info("processed txn:~n ~p ~p~n", [TxId, TxMaterializedKeysWithOps]),
    ?READ_TXN(TxId, DcId, TxCommitTime, TxOffset, TxMaterializedKeysWithOps).

prepare_txn_operations(TxId, aborted) ->
    ?READ_TXN(TxId, none, none, none, aborted).

preprocess_comitted(L) ->
    L.

notify_client([], Data) ->
    {ok, Data};
notify_client(FinalyzedTxns, #data{port = Port, last_send_map = SM} = Data) ->
    case notify_client0(FinalyzedTxns, undefined, Port, SM) of
        {ok, LastTxId, SM1} ->
            {ok, Data#data{last_read_txid = LastTxId,
                           last_send_map = SM1,
                           to_send = []
                          }};
        {retry, NotSendTxns, SM1} ->
            {retry, set_port_send_timer(
                      Data#data{to_send = NotSendTxns,
                                last_send_map = SM1
                               })};
        {error, Reason} ->
            {error, Reason}
    end.

notify_client0(D = [?READ_TXN(TxId, DcId, TxCommitTime, TxOffset, TxOpsList)
                   | FinalyzedTxns], _LastTxn, Port, SM)
 when is_list(TxOpsList)->

    case
        vx_wal_tcp_worker:send(Port, TxId, TxOffset, TxOpsList)
    of
        false ->
            %% We need to retry later, port is busy
            {retry, D, SM};
        true ->
            SM1 = SM#{ DcId => TxCommitTime },
            notify_client0(FinalyzedTxns, TxId, Port, SM1);
        {error, Reason} ->
            {error, Reason}
    end;
notify_client0([?READ_TXN(TxId, none, none, none, aborted)
               | FinalyzedTxns], _LastTxn, Port, SM) ->
    notify_client0(FinalyzedTxns, TxId, Port, SM);
notify_client0([], LastTxn, _, SM) ->
    {ok, LastTxn, SM}.

-ifdef(TEST).

-endif.
