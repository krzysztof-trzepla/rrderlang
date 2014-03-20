%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014, ACK CYFRONET AGH
%%% @doc
%%%
%%% @end
%%% Created : 16. Mar 2014 8:09 PM
%%%-------------------------------------------------------------------
-module(rrderlang).
-author("Krzysztof Trzepla").

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([create/4,
  fetch/3,
  update/3,
  update/4,
  graph/2]).

%% gen_server callbacks
-export([init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3]).

-define(SERVER, ?MODULE).
-define(SEPARATOR, <<" ">>).
-define(TIMEOUT, 2000).

-record(state, {port}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
  {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Create Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(create(Filename :: binary(), Options :: binary(), DSs :: [binary()], RRAs :: [binary()]) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
create(Filename, Options, DSs, RRAs) ->
  try
    Command = format([<<"create">>, Filename, Options, format(DSs), format(RRAs), <<"\n">>]),
    gen_server:call(?MODULE, {command, Command}, ?TIMEOUT)
  catch
    Error:Reason ->
      {Error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Update Round Robin Database using current timestamp.
%%
%% @end
%%--------------------------------------------------------------------
-spec(update(Filename :: binary(), Options :: binary(), Values :: [float()]) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
update(Filename, Options, Values) ->
  update(Filename, Options, Values, <<"N">>).

%%--------------------------------------------------------------------
%% @doc
%% Update Round Robin Database using given timestamp.
%%
%% @end
%%--------------------------------------------------------------------
-spec(update(Filename :: binary(), Options :: binary(), Values :: [float()] | [integer()], Timestamp :: integer() | binary()) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
update(Filename, Options, Values, Timestamp) when is_integer(Timestamp) ->
  update(Filename, Options, Values, integer_to_binary(Timestamp));
update(Filename, Options, Values, Timestamp) ->
  try
    BinaryValues = lists:map(fun
      (Elem) when is_float(Elem) -> float_to_binary(Elem);
      (Elem) when is_integer(Elem) -> integer_to_binary(Elem);
      (_) -> <<"U">> end, Values),
    Command = format([<<"update">>, Filename, Options, format([Timestamp | BinaryValues], <<":">>), <<"\n">>]),
    gen_server:call(?MODULE, {command, Command}, ?TIMEOUT)
  catch
    Error:Reason ->
      {Error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Fetch data Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(fetch(Filename :: binary(), Options :: binary(), CF :: binary()) ->
  {ok, Result :: {Timestamp :: integer(), [[float()]]}} |
  {error, Error :: term()}).
fetch(Filename, Options, CF) ->
  try
    Command = format([<<"fetch">>, Filename, CF, Options, <<"\n">>]),
    case gen_server:call(?MODULE, {command, Command}, ?TIMEOUT) of
      {ok, [BinaryHeader, _EmptyLine | BinaryData]} ->
        Header = lists:filter(fun(Elem) -> Elem =/= <<>> end, binary:split(BinaryHeader, ?SEPARATOR, [global])),
        Data = lists:map(fun(BinaryElem) ->
          [BinaryTimestamp | BinaryValues] = binary:split(BinaryElem, ?SEPARATOR, [global]),
          Size = size(BinaryTimestamp) - 1,
          <<BinaryTimestampNoColon:Size/binary, _/binary>> = BinaryTimestamp,
          Timestamp = binary_to_integer(BinaryTimestampNoColon),
          Values = lists:map(fun(Elem) when Elem =:= <<"-nan">> -> nan;
            (Elem) -> binary_to_float(Elem) end, BinaryValues),
          {ok, {Timestamp, Values}}
        end, BinaryData),
        {ok, {Header, Data}};
      {error, Error} ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%%--------------------------------------------------------------------
%% @doc
%% Create graphical representation of data from Round Robin Database.
%%
%% @end
%%--------------------------------------------------------------------
-spec(graph(Filename :: binary(), Options :: binary()) ->
  {ok, Result :: binary()} |
  {error, Error :: term()}).
graph(Filename, Options) ->
  try
    Command = format([<<"graph">>, Filename, Options]),
    gen_server:call(?MODULE, {command, Command}, ?TIMEOUT)
  catch
    _:Reason ->
      {error, Reason}
  end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
  {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term()} | ignore).
init([]) ->
  process_flag(trap_exit, true),
  case os:find_executable("rrdtool") of
    false ->
      {stop, no_rrdtool};
    RRDtool ->
      Port = open_port({spawn_executable, RRDtool}, [{line, 1024}, {args, ["-"]}, binary]),
      {ok, #state{port = Port}}
  end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
  {reply, Reply :: term(), NewState :: #state{}} |
  {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_call({command, Command}, _From, State) ->
  port_command(State#state.port, Command),
  case receive_data(State#state.port, []) of
    {ok, Data} -> {reply, {ok, Data}, State};
    {error, Error} -> {reply, {error, Error}, State}
  end;
handle_call(_Request, _From, State) ->
  {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
  {noreply, NewState :: #state{}} |
  {noreply, NewState :: #state{}, timeout() | hibernate} |
  {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, State) ->
  port_command(State#state.port, [<<"quit\n">>]),
  port_close(State#state.port),
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
  {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

format(Command) ->
  format(Command, ?SEPARATOR).

format([], _) ->
  <<>>;
format([Command | Commands], Separator) ->
  lists:foldl(fun(Elem, Acc) -> <<Acc/binary, Separator/binary, Elem/binary>> end, Command, Commands).

receive_data(Port, Acc) ->
  receive
    {Port, {data, {eol, <<"OK", _/binary>>}}} ->
      {ok, lists:reverse(Acc)};
    {Port, {data, {eol, <<"ERROR: ", Error/binary>>}}} ->
      {error, Error};
    {Port, {data, {eol, Data}}} ->
      receive_data(Port, [Data | Acc]);
    Other ->
      {error, Other}
  end.