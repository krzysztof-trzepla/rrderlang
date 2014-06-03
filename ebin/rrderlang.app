%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2014, ACK CYFRONET AGH
%%% @doc
%%%
%%% @end
%%% Created : 16. Mar 2014 8:07 PM
%%%-------------------------------------------------------------------
{application, rrderlang, [
  {description, "RRD Erlang binding"},
  {vsn, "1.0.0"},
  {registered, []},
  {applications, [
    kernel,
    stdlib
  ]},
  {modules, [rrderlang, rrderlang_app, rrderlang_sup]},
  {mod, {rrderlang_app, []}},
  {env, []}
]}.
