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

-record(state, {ref :: reference(),
                data_root :: string(),
                config :: config(),
                read_opts = [],
                write_opts = [],
                fold_opts = [{fill_cache, false}]
               }).

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
    DataDir = filename:join(app_helper:get_prop_or_env(data_root, Config, sqlite), integer_to_list(Partition)),
    sqlite3:open(DataDir, {file, DataDir}).
    % create table if not exists

%% @doc Stop the sqlite backend
stop(_State) ->
    ok.

%%%% @doc Retrieve an object from the sqlite backend
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(_Bucket, _Key, State) ->
    {ok, <<"dummy value">>, State}.

%% @doc Insert an object into the sqlite backend.
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(_Bucket, _Key, _IndexSpecs, _Val, State) ->
    {ok, State}.
%%    StorageKey = to_object_key(Bucket, Key),
%%    sqlite3:exec().
%%
%%%% @doc Delete an object from the sqlite backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(_Bucket, _Key, _IndexSpecs, State) ->
    {ok, State}.
%%
%%    %% Create the KV delete...
%%
%% @doc Delete all objects from this sqlite backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(State) ->
    {ok, State}.

%%    case sqlite3:destroy(DataRoot, []) of
%%        ok ->
%%            case sqlite3:open(DataRoot, [{file, DataRoot}]) of
%%                {ok, Ref} ->
%%                    {ok, State#state { ref = Ref }};
%%                {error, Reason} ->
%%                    {error, Reason, State}
%%            end;
%%        {error, Reason} ->
%%            {error, Reason, State}
%%    end.
%%

%% @doc Returns true if this sqlite backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(_State) ->
    true.
    %sqlite3:is_empty(Ref).

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
fold_buckets(_FoldBucketsFun, _Acc, _Opts, _State) ->
    {ok, []}.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(_FoldKeysFun, _Acc, _Opts, _State) ->
    {ok, []}.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(_FoldObjectsFun, _Acc, _Opts, _State) ->
    {ok, []}.

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
