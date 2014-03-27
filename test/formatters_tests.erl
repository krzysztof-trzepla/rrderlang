%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014, ACK CYFRONET AGH
%%% @doc This module tests the functionality of formatters module.
%%% It contains unit tests that base on eunit.
%%%
%%% @end
%%% Created : 25. Mar 2014 20:07
%%%-------------------------------------------------------------------
-module(formatters_tests).
-author("Krzysztof Trzepla").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include("common.hrl").
-endif.

-ifdef(TEST).

%% ===================================================================
%% Tests description
%% ===================================================================

formatters_test_() ->
  {foreach,
    fun setup/0,
    fun teardown/1,
    [
      {"default formatter should pass", fun default_formatter_should_pass/0},
      {"default formatter with 'nan' should pass", fun default_formatter_with_nan_should_pass/0},
      {"default formatter should fail", fun default_formatter_should_fail/0},
      {"json formatter should pass", fun json_formatter_should_pass/0},
      {"json formatter with 'nan' should pass", fun json_formatter_with_nan_should_pass/0},
      {"json formatter should fail", fun json_formatter_should_fail/0}
    ]
  }.

%% ===================================================================
%% Setup/teardown functions
%% ===================================================================

setup() ->
  ok.

teardown(_) ->
  ok.

%% ===================================================================
%% Tests functions
%% ===================================================================

default_formatter_should_pass() ->
  BinaryInput = [
    <<"3000000000: 3.0 3.0 3.0">>,
    <<"2000000000: 2.0 2.0 2.0">>,
    <<"1000000000: 1.0 1.0 1.0">>,
    <<>>,
    <<"first second third">>
  ],
  ExprectedOutput = {
    ok,
    {
      [<<"first">>, <<"second">>, <<"third">>],
      [
        {1000000000, [1.0, 1.0, 1.0]},
        {2000000000, [2.0, 2.0, 2.0]},
        {3000000000, [3.0, 3.0, 3.0]}
      ]
    }
  },
  Output = formatters:default_formatter(BinaryInput),
  ?assertEqual(ExprectedOutput, Output).

default_formatter_with_nan_should_pass() ->
  BinaryInput = [
    <<"3000000000: 3.0 3.0 -nan">>,
    <<"2000000000: 2.0 -nan 2.0">>,
    <<"1000000000: -nan 1.0 1.0">>,
    <<>>,
    <<"first second third">>
  ],
  ExprectedOutput = {
    ok,
    {
      [<<"first">>, <<"second">>, <<"third">>],
      [
        {1000000000, [nan, 1.0, 1.0]},
        {2000000000, [2.0, nan, 2.0]},
        {3000000000, [3.0, 3.0, nan]}
      ]
    }
  },
  Output = formatters:default_formatter(BinaryInput),
  ?assertEqual(ExprectedOutput, Output).

default_formatter_should_fail() ->
  BinaryInput = [
    <<"3000000000: 3.0 3.0 3.0">>,
    <<"2000000000: 2.0 2.0 2.0">>,
    <<"1000000000: 1.0 1.0 1.0">>,
    <<"first second third">>
  ],
  ExprectedOutput = {error, format_error},
  Output = formatters:default_formatter(BinaryInput),
  ?assertEqual(ExprectedOutput, Output).

json_formatter_should_pass() ->
  BinaryInput = [
    <<"3000000000: 3.0 3.0 3.0">>,
    <<"2000000000: 2.0 2.0 2.0">>,
    <<"1000000000: 1.0 1.0 1.0">>,
    <<>>,
    <<"first second third">>
  ],
  ExprectedOutput = {ok, "{\"cols\": [{\"label\": \"Timestamp\", \"type\": \"date\"}, {\"label\": \"first\", \"type\": \"number\"}, {\"label\": \"second\", \"type\": \"number\"}, {\"label\": \"third\", \"type\": \"number\"}], \"rows\": [{\"c\": [{\"v\": \"Date(1000000000000)\"}, {\"v\": 1.0}, {\"v\": 1.0}, {\"v\": 1.0}]}, {\"c\": [{\"v\": \"Date(2000000000000)\"}, {\"v\": 2.0}, {\"v\": 2.0}, {\"v\": 2.0}]}, {\"c\": [{\"v\": \"Date(3000000000000)\"}, {\"v\": 3.0}, {\"v\": 3.0}, {\"v\": 3.0}]}]}"},
  Output = formatters:json_formatter(BinaryInput),
  ?assertEqual(ExprectedOutput, Output).

json_formatter_with_nan_should_pass() ->
  BinaryInput = [
    <<"3000000000: 3.0 3.0 -nan">>,
    <<"2000000000: 2.0 -nan 2.0">>,
    <<"1000000000: -nan 1.0 1.0">>,
    <<>>,
    <<"first second third">>
  ],
  ExprectedOutput = {ok, "{\"cols\": [{\"label\": \"Timestamp\", \"type\": \"date\"}, {\"label\": \"first\", \"type\": \"number\"}, {\"label\": \"second\", \"type\": \"number\"}, {\"label\": \"third\", \"type\": \"number\"}], \"rows\": [{\"c\": [{\"v\": \"Date(1000000000000)\"}, {\"v\": \"null\"}, {\"v\": 1.0}, {\"v\": 1.0}]}, {\"c\": [{\"v\": \"Date(2000000000000)\"}, {\"v\": 2.0}, {\"v\": \"null\"}, {\"v\": 2.0}]}, {\"c\": [{\"v\": \"Date(3000000000000)\"}, {\"v\": 3.0}, {\"v\": 3.0}, {\"v\": \"null\"}]}]}"},
  Output = formatters:json_formatter(BinaryInput),
  ?assertEqual(ExprectedOutput, Output).

json_formatter_should_fail() ->
  BinaryInput = [
    <<"3000000000: 3.0 3.0 3.0">>,
    <<"2000000000: 2.0 2.0 2.0">>,
    <<"1000000000: 1.0 1.0 1.0">>,
    <<"first second third">>
  ],
  ExprectedOutput = {error, format_error},
  Output = formatters:json_formatter(BinaryInput),
  ?assertEqual(ExprectedOutput, Output).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-endif.