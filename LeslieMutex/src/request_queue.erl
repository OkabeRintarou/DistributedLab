-module(request_queue).

-export([new/0, put/2, remove/2, is_first/2]).

-record(state, {lists}).

new() ->
	#state{lists=[]}.

put(Msg, #state{lists=List}) ->
	#state{lists=insert(Msg, List, [])}.

insert(Msg, [], CurList) ->
	CurList ++ [Msg];
insert({Timestamp, ServerRef} = Msg, [{T, S} | Tail] = List, CurList) ->
	if
		Timestamp < T ->
			lists:append([CurList, [Msg], List]);
		Timestamp > T ->
			insert(Msg, Tail, CurList ++ [{T, S}]);
		Timestamp =:= T ->
			if 
				ServerRef < S ->
				   lists:append([CurList, [Msg], List]);
				S < ServerRef ->
					insert(Msg, Tail, CurList ++ [{T, S}]);
				true ->
					throw({duplicate_message, Msg})
			end
	end.


remove(Msg, #state{lists=List}) ->
	#state{lists=remove(Msg, List, [])}.

remove(_, [], List) ->
	List;
remove({Timestamp, ServerRef} = Msg, [{T, S}|Tail], List) ->
	if
		Timestamp =:= T andalso ServerRef =:= S ->
			List ++ Tail;
		true ->
			remove(Msg, Tail, List ++ [{T, S}])
	end.

is_first(ServerRef, #state{lists=List}) ->
	is_first_aux(ServerRef, List).

is_first_aux(_, []) ->
	false;
is_first_aux(ServerRef, [{T, S} | _]) ->
	case ServerRef =:= S of
		true ->
			{ok, T};
		false ->
			false
	end.
