%%%-------------------------------------------------------------------
%%% @author  <valinurovam@gmail.com>
%%% @copyright (C) 2012, 
%%% @doc
%%%
%%% @end
%%% Created :  3 Aug 2012 by  <valinurovam@gmail.com>
%%%-------------------------------------------------------------------
-module(status_server).

-behaviour(gen_server).

%% API
-export([
		start_link/0, 
		stop/0, 
		update/0, 
		process_status/1
		]).

%% gen_server callbacks
-export([
		init/1, 
		handle_call/3, 
		handle_cast/2, 
		handle_info/2,
		terminate/2, 
		code_change/3
		]).

	 
%%%===================================================================
%%% DEFINES
%%%===================================================================
-define(SERVER, ?MODULE). 
-define(STATUS_TABLE, ?MODULE).

-define(DATABASE_NAME, "mbstat").
-define(USER_NAME, "mbuser").
-define(USER_PASS, "mbuser").
-define(PORT, 3306).
-define(HOST, "localhost").
-define(DB, db).
-define(CHARSET, utf8).


%%%===================================================================
%%% RECORDS
%%%===================================================================
-record(status_record, {id, partner_id, message_id, pdu_id, url, status}).


%%--------------------------------------------------------------------
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
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

stop() -> 
	gen_server:call(?MODULE, stop).

update() ->
	gen_server:call(?SERVER, update).
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
init([]) ->
	process_flag(trap_exit, true),
	
	emysql:add_pool(?DB, 10,
        ?USER_NAME, ?USER_PASS, ?HOST, ?PORT,
        ?DATABASE_NAME, ?CHARSET),
	
	Table = ets:new(?SERVER, [public,named_table]),
	lager:error("Error message", []),
	self() ! update,
	
	lager:alert("~p starting~n", [?SERVER]),
	io:format("~p starting~n", [?SERVER]),
    {ok, Table}.

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
handle_call(stop, _From, State) ->
	emysql:remove_pool(?DB),
	{stop, normal, stopped, State};
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
handle_info(update, State) ->
	self() ! {update, status_count_to_send()},
	{noreply, State};

handle_info({update, StatusCount}, State) when StatusCount > 500 ->
	erlang:send_after(1000, self(), update),
	{noreply, State};

handle_info({update, _StatusCount}, State) ->
	StatusRecords = get_status_records(),
    CountOfStatuses = insert_status_recors(StatusRecords),
	case CountOfStatuses > 0 of
		true ->
			lager:info("Updating! Insert into ~p table ~p statuses~n", [?SERVER, CountOfStatuses]);
		_ -> ok
	end,
	self() ! {send, ets:first(?STATUS_TABLE)},
	{noreply, State};

handle_info(delete, State) ->
	case ets:info(?STATUS_TABLE) of
		undefined ->
			ok;
		_TableInfo ->		
			ets:match_delete(?STATUS_TABLE, {'$1',{'$2',-1}})
	end,
	erlang:send_after(1000, self(), update),
	{noreply, State};

handle_info(start_sender, State) ->
	self() ! {send, ets:first(?STATUS_TABLE)},
	{noreply, State};

handle_info({send, '$end_of_table'}, State) ->
	self() ! delete,
	{noreply, State};
	
handle_info({send, Id}, State) ->
	case ets:lookup(?STATUS_TABLE, Id) of
		[{Id, {Status, 0}}] -> 
			ets:insert(?STATUS_TABLE, {Id, {Status, 1}}),
			spawn(?MODULE, process_status, [Status]);
		[_] -> ok;
		[] -> ok
	end,
	self() ! {send, ets:next(?STATUS_TABLE, Id)},
	{noreply, State};	
	
handle_info(Info, State) ->
	lager:alert("Info msg: ~p~n", [Info]),
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
terminate(_Reason, _State) ->
	lager:alert("~p stopping~n", [?SERVER]),
	
	io:format("~p stopping~n", [?SERVER]),
    ok.

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



%% Функция получает все записи, готовые к отправке
%% Записи возвращаются в виде списка
get_status_records() ->
	try
		Result = emysql:execute(?DB, 
		<<"select id, partner_id, message_id, pdu_id, url, status 
			from partner_out_message_dlr where status = 0 limit 500">>),
		emysql_util:as_record(Result, status_record, record_info(fields, status_record))
	catch
		Error:Reason ->
			lager:error("Error on update DB. Error: ~p Reason: ~p~n", [Error, Reason]),
			[]
	end.


%% Вставляет все записи, готовые к отправке, в 
%% общую таблицу, которая используется всем сервером.
insert_status_recors([]) -> 0;
insert_status_recors([Status|StatusList]) ->
	insert_status_records([Status|StatusList], 0).
insert_status_records([Status|StatusList], InsertCount) when InsertCount < 100 ->
	#status_record{id = Id} = Status,
	case ets:lookup(?STATUS_TABLE, Id) of
		[] ->
			ets:insert(?STATUS_TABLE, {Id, {Status, 0}}),
			insert_status_records(StatusList, InsertCount+1);
		[_Key] -> insert_status_records(StatusList, InsertCount)
	end;
insert_status_records(_StatusList, InsertCount) when InsertCount >= 100 -> InsertCount;
insert_status_records([], InsertCount) -> InsertCount.
		

%% Возвращает количество не обработанных записей.
status_count_to_send() ->
	case ets:info(?STATUS_TABLE) of
		undefined ->
			0;
		_TableInfo ->
			ets:select_count(?STATUS_TABLE, [{ {'$1', {'$2', '$3'}}, [{ '=:=', '$3', 0}], [true]}])		
	end.
	
process_status(#status_record{id = Id, url = HttpUrl} = Status) ->
	HttpURL = binary_to_list(HttpUrl),
	case httpc:request(HttpURL) of
		{ok, {{_Version, HttpStatus, ReasonPhrase}, _Headers, _Body}} ->
			case HttpStatus of
				200 ->
					%% удаляем сообщение
					IdStr = erlang:integer_to_list(Id),
					emysql:execute(?DB, "delete from partner_out_message_dlr where id = " ++ IdStr),
					
					%% отмечаем для удаления из ETS
					ets:insert(?STATUS_TABLE, {Id, {Status, -1}}),
					lager:info("~p ~p ~p~n", [HttpURL, HttpStatus, ReasonPhrase]);
				_OtherStatus ->
					%% отмечаем для повторной отработки
					ets:insert(?STATUS_TABLE, {Id, {Status, 0}}),
					lager:notice("~p ~p ~p~n", [HttpURL, HttpStatus, ReasonPhrase])
			end;
		OtherError ->
			ets:insert(?STATUS_TABLE, {Id, {Status, 0}}),
			lager:error("~p ~p~n", [HttpURL, OtherError])
	end.