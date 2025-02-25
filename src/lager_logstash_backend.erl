%% Copyright (c) 2014 Krzysztof Rutka
%%
%% Permission is hereby granted, free of charge, to any person obtaining a
%% copy of this software and associated documentation files (the "Software"),
%% to deal in the Software without restriction, including without limitation
%% the rights to use, copy, modify, merge, publish, distribute, sublicense,
%% and/or sell copies of the Software, and to permit persons to whom the
%% Software is furnished to do so, subject to the following conditions:
%%
%% The above copyright notice and this permission notice shall be included in
%% all copies or substantial portions of the Software.
%%
%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
%% FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
%% IN THE SOFTWARE.

%% @author Krzysztof Rutka <krzysztof.rutka@gmail.com>
-module(lager_logstash_backend).
-behaviour(gen_event).

-export([init/1]).
-export([handle_event/2]).
-export([handle_call/2]).
-export([handle_info/2]).
-export([terminate/2]).
-export([code_change/3]).

-export_type([output/0]).

-define(DEFAULT_LEVEL, info).
-define(DEFAULT_OUTPUT, {tcp, "localhost", 5000}).
-define(DEFAULT_ENCODER, jsx).
-define(DEFAULT_TAG, undefined).

-type output() :: tcp() | udp() | file().
-type tcp() :: {tcp, inet:hostname(), inet:port_number()} |
               {tcp, inet:hostname(), inet:port_number(), non_neg_integer() | infinity}.
-type udp() :: {udp, inet:hostname(), inet:port_number()}.
-type file() :: {file, string()}.
-type configuration() :: #{ atom() => term() }.

-record(state, {
          worker :: pid() | undefined,
          monitor :: reference() | undefined,
          level :: lager:log_level_number(),
          output :: output(),
          config :: configuration()
         }).

init(Args) ->
    {ok, _} = application:ensure_all_started(lager_logstash),
    Level = arg(level, Args, ?DEFAULT_LEVEL),
    LevelNumber = lager_util:level_to_num(Level),
    Output = arg(output, Args, ?DEFAULT_OUTPUT),
    Encoder = arg(json_encoder, Args, ?DEFAULT_ENCODER),
    Config = case read_tag(arg(tag, Args, ?DEFAULT_TAG)) of
                 undefined ->
                     #{ json_encoder => Encoder,
                        tag => undefined };
                 T ->
                     #{ json_encoder => Encoder,
                        tag => T }
    end,

    {ok, create_worker(#state{
        output = Output,
        config = Config,
        level = LevelNumber }) }.

arg(Name, Args, Default) ->
    case lists:keyfind(Name, 1, Args) of
        {Name, Value} -> Value;
        false         -> Default
    end.

handle_call({set_loglevel, Level}, State) ->
    LevelNumber = lager_util:level_to_num(Level),
    {ok, ok, State#state{level = LevelNumber}};
handle_call(get_loglevel, #state{level = LevelNumber} = State) ->
    Level = lager_util:num_to_level(LevelNumber),
    {ok, Level, State};
handle_call(_Request, State) ->
    {ok, ok, State}.

handle_event({log, _}, #state{worker = undefined} = State) ->
    {ok, State};
handle_event({log, Message}, State) ->
    _ = handle_log(Message, State),
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_log(LagerMsg, #state{level = Level, config = Config} = State) ->
    Severity = lager_msg:severity(LagerMsg),
    case lager_util:level_to_num(Severity) =< Level of
        true ->
            Payload = lager_logstash_json_formatter:format(LagerMsg, Config),
            send_log(Payload, State);
        false -> skip
    end.

handle_info({'DOWN', Mon, _, _, shutdown}, #state { monitor = Mon } = State) ->
    {ok, State};
handle_info({'DOWN', Mon, _, _, {shutdown, _}}, #state { monitor = Mon } = State) ->
    {ok, State};
handle_info({'DOWN', Mon, _, _, _}, #state { monitor = Mon } = State) ->
    {ok, create_worker(State)};
handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, #state{ monitor = Mon, worker = Pid }) ->
    erlang:demonitor(Mon, [flush]),
    lager_logstash_worker:stop(Pid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

send_log(_Payload, #state{worker = undefined}) -> ok;
send_log(Payload, #state{worker = Worker}) ->
    gen_server:cast(Worker, {log, Payload}).

create_worker(#state { output = Output } = State) ->
    {ok, WorkerPid} = lager_logstash_sup:start_worker(Output),
    Mon = erlang:monitor(process, WorkerPid),
    State#state {
        monitor = Mon,
        worker = WorkerPid
    }.

read_tag(undefined) -> undefined;
read_tag(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
read_tag(L) when is_list(L) ->
    valid_tag_list(L).

valid_tag_list([]) -> [];
valid_tag_list([{K, V} | KVs]) -> 
    ok = valid_key(K),
    ok = valid_value(V),
    [{K,V} | valid_tag_list(KVs)].

valid_key(A) when is_atom(A) -> ok;
valid_key(B) when is_binary(B) -> ok.

valid_value(B) when is_binary(B) -> ok;
valid_value(S) when is_list(S) -> is_string(S).

is_string([]) -> ok;
is_string([C|Cs]) when C >= 0 andalso C < 256 -> is_string(Cs).
    
