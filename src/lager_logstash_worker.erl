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

%% @author Chap Lovejoy <chaplovejoy@gmail.com>

-module(lager_logstash_worker).

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-type handle() :: gen_tcp:socket() | gen_udp:socket() | file:fd().

-record(state, {
          handle :: handle() | undefined,
          host :: inet:hostname() | undefined,
          port :: inet:port_number() | undefined,
          type :: tcp | udp | file
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, Output) ->
    gen_server:start_link({local, Name}, ?MODULE, [Output], []).

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
init([Output]) ->
    State = connect(Output),
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(shutdown, State) ->
    {stop, normal, State};
handle_cast({log, Payload}, State) ->
    ok = send_log(Payload, State),
    {noreply, State};
handle_cast(_Msg, State) ->
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
terminate(_Reason, #state{handle = undefined}) -> ok;
terminate(_Reason, #state{type = tcp, handle = Socket}) ->
    ok = gen_tcp:close(Socket);
terminate(_Reason, #state{type = udp, handle = Socket}) ->
    ok = gen_udp:close(Socket);
terminate(_Reason, #state{type = file, handle = Fd}) ->
    ok = file:close(Fd).

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

connect({tcp, Host, Port}) ->
    Opts = [binary, {active, false}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, Opts) of
        {ok, Socket} -> #state{type = tcp, handle = Socket};
        {error, _}   -> #state{handle=undefined}
    end;
connect({tcp, Host, Port, Timeout}) ->
    Opts = [binary, {active, false}, {keepalive, true}],
    case gen_tcp:connect(Host, Port, Opts, Timeout) of
        {ok, Socket} -> #state{type = tcp, handle = Socket};
        {error, _}   -> #state{handle=undefined}
    end;
connect({udp, Host, Port}) ->
    Opts = [binary],
    case gen_udp:open(0, Opts) of
        {ok, Socket} -> #state{type = udp, host = Host, port = Port, handle = Socket};
        {error, _}   -> #state{handle=undefined}
    end;
connect({file, Path}) ->
    case file:open(Path, [append]) of
        {ok, Fd}   -> #state{type=file, handle = Fd};
        {error, _} -> #state{handle=undefined}
    end.

send_log(Payload, #state{type = tcp, handle = Socket}) ->
    ok = gen_tcp:send(Socket, Payload);
send_log(Payload, #state{type = udp, host = Host, port = Port, handle = Socket}) ->
    ok = gen_udp:send(Socket, Host, Port, Payload);
send_log(Payload, #state{type = file, handle = Fd}) ->
    ok = file:write(Fd, Payload).

