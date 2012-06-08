-module(riak_kv_sqlite_backend).
-behavior(riak_kv_backend).

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(API_VERSION, 1).
% -define(CAPABILITIES, [async_fold, indexes]).
-define(CAPABILITIES, []).

-record(state, {ref :: reference()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the sqlite backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, sqlite3), integer_to_list(Partition)),
    filelib:ensure_dir(DataDir),
    %io:fwrite(DataDir),
    %io:fwrite("\n"),
    %{ok, Pid} = sqlite3:open(file, [{file, DataDir}]),
    %io:fwrite("Trying to open sqlite3 database...\n"),
    Db = list_to_atom(integer_to_list(Partition)),
    {ok, Pid} = sqlite3:open(Db, [{file, DataDir}]),
    %io:fwrite("database opened.\n"),
    ok = sqlite3:sql_exec(Db,
        "CREATE TABLE IF NOT EXISTS store (
            bucket BLOB,
            key BLOB,
            value BLOB,
            CONSTRAINT store_primary_key PRIMARY KEY (bucket, key)
        );"
    ),
    %% TODO prepare some statements
    %sqlite3:create_table(Db, store, [{bucket, blob}, {key, blob}, {value, blob}]),
    {ok, #state{ref=Pid}}.
    %% io:write(Partition),
    %% io:fwrite("\n"),
    %% io:write(Config),
    %% io:fwrite("\n"),
    %% DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, sqlite3), integer_to_list(Partition)),
    %% io:fwrite(DataDir),
    %% io:fwrite("\n"),
    %{ok, Pid} = sqlite3:open(DataDir, {file, DataDir}),
    %{ok, #state{ref=Pid}},
    % create table if not exists

%% @doc Stop the sqlite backend
stop(_State) ->
    ok.

%%%% @doc Retrieve an object from the sqlite backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(Bucket, Key, #state{ref=Ref}=State) ->
    io:write(State),
    io:fwrite("\n"),
    case sqlite3:sql_exec(Ref, "SELECT value FROM store WHERE bucket = ? AND key = ?",
        [{1, Bucket}, {2, Key}]) of
        [{columns,_},{rows,[{Value}]}] ->
            io:fwrite("Value: ~80p~n", [Value]),
            {ok, Value, State};
        Result ->
            io:fwrite("Result: ~80p~n", [Result]),
            {error, not_found, State}
    end.

%% @doc Insert an object into the sqlite backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(Bucket, Key, IndexSpecs, Val, #state{ref=Ref}=State) ->
    io:fwrite("Bucket: ~80p~n", [Bucket]),
    io:fwrite("Key: ~80p~n", [Key]),
    io:fwrite("IndexSpecs: ~80p~n", [IndexSpecs]),
    io:fwrite("Val: ~80p~n", [Val]),
    io:fwrite("Ref: ~80p~n", [Ref]),
    case sqlite3:sql_exec(Ref,
            "INSERT OR REPLACE INTO store (bucket, key, value) VALUES (?, ?, ?);",
            [{1, Bucket}, {2, Key}, {3, Val}]) of

                {rowid, _} ->
                    {ok, State};
                [_, _, Error] ->
                    {error, Error, State}
    end.

%%%% @doc Delete an object from the sqlite backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, #state{ref=Ref}=State) ->
    case sqlite3:sql_exec(Ref,
            "DELETE FROM store WHERE bucket = ? AND key = ?;",
            [{1, Bucket}, {2, Key}]) of
                ok ->
                    {ok, State};
                [_, _, Error] ->
                    {error, Error, State}
    end.

%% @doc Delete all objects from this sqlite backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{ref=Ref}=State) ->
    case sqlite3:sql_exec(Ref, "DELETE FROM store;") of
            ok ->
                {ok, State};
            _ ->
                {error, "An error.", State}
    end.

%% @doc Returns true if this sqlite backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{ref=Ref}) ->
    % http://www.mail-archive.com/sqlite-users@sqlite.org/msg19151.html
    case sqlite3:sql_exec(Ref, "SELECT (SELECT bucket FROM store LIMIT 1) IS NOT NULL;") of
        [{columns,_},{rows,[{Value}]}] ->
            Value;
        _ ->
            {error, "Error in is_empty()"}
    end.

%% @doc Get the status information for this eleveldb backend
-spec status(state()) -> [{atom(), term()}].
status(_State) ->
    [{stats, ok}].

%%%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.


%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, _Opts, #state{ref=Ref}) ->
    FoldFun = fun ({Bucket}, Acc2) ->
            FoldBucketsFun(Bucket, Acc2)
    end,
    %TODO implement async fold
    [{columns, ["bucket"]}, {rows, Rows}] = sqlite3:sql_exec(Ref,
        "SELECT DISTINCT bucket FROM store;"
    ),
    Result = lists:foldl(FoldFun, Acc, Rows),
    {ok, Result}.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{ref=Ref}) ->

    Bucket = lists:keyfind(bucket, 1, Opts),
    BucketKeys = if
        %TODO implement index queries
        Bucket /= false ->
            {bucket, FilterBucket} = Bucket,
            [{columns, ["bucket", "key"]},{rows, Rows}] = sqlite3:sql_exec(Ref,
                "SELECT bucket, key FROM store WHERE bucket = ?",
                [{1, FilterBucket}]
            ),
            Rows;
        true ->
            [{columns, ["bucket", "key"]},{rows, Rows}] = sqlite3:sql_exec(Ref,
                "SELECT bucket, key FROM store"
            ),
            Rows
    end,
    Folder = fun ({Bucket2, Key2}, Acc2) ->
            FoldKeysFun(Bucket2, Key2, Acc2)
    end,
    %TODO implement async fold
    Result = lists:foldl(Folder, Acc, BucketKeys),
    {ok, Result}.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{ref=Ref}) ->
    %TODO implement async fold
    Bucket = proplists:get_value(bucket, Opts),
    BucketsKeysValues = if
        Bucket /= false ->
            {bucket, FilterBucket} = Bucket,
            [{columns, ["bucket", "key", "value"]}, {rows, Rows}] = sqlite3:sql_exec(
                Ref,
                "SELECT bucket, key, value FROM store WHERE bucket = ?;",
                [{1, FilterBucket}]
            ),
            Rows;
        true ->
            [{colums, ["bucket", "key", "value"]}, {rows, Rows}] = sqlite3:sql_exec(
                Ref,
                "SELECT bucket, key, value FROM store;"
            ),
            Rows
    end,
    FoldFun = fun ({Bucket2, Key2, Value2}, Acc2) ->
            FoldObjectsFun(Bucket2, Key2, Value2, Acc2)
    end,
    Result = lists:foldl(FoldFun, Acc, BucketsKeysValues),
    {ok, Result}.
%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEXT).

simple_test_() ->
    ?assertCmd("rm -rf test/sqlite-backend"),
    application:set_env(sqlite, data_root, "test/sqlite-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

-endif.

-ifdef(EQC).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 60000,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/sqlite-backend"},
                                         {async_folds, false}]))]},
          {timeout, 60000,
            [?_assertEqual(true,
                          backend_eqc:test(?MODULE, false,
                                           [{data_root,
                                             "test/sqlite-backend"}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, {file, "riak_kv_sqlite_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_sqlite_backend_eqc.log"}),

    ok.

cleanup(_) ->
    ?_assertCmd("rm -rf test/sqlite-backend").

-endif. % EQC
