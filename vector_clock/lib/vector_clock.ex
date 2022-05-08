defmodule VectorClock do
  @type counter :: integer
  @type timestamp :: integer
  @type vclock_node :: term
  @type dot :: {vclock_node, {counter, timestamp}}
  @type pure_dot :: {vclock_node, counter}
  @type vclock :: [dot]

  @doc """
  Create a brand new vclock
  """
  @spec fresh() :: vclock
  def fresh(), do: []

  @spec fresh(vclock_node, counter) :: vclock
  def fresh(node, count) do
    [{node, {count, timestamp()}}]
  end

  @doc """
  Return true if Va is a direct descendant og Vb, else false -- remember, a vclock is its own descendant!
  """
  @spec descends(vclock, vclock) :: boolean
  # all vclocks descend from the empty vclock
  def descends(_, []), do: true

  def descends(va, vb) do
    Enum.all?(vb, fn {nodeb, {cb, _}} ->
      case Enum.find(va, nil, fn {nodea, _} -> nodea == nodeb end) do
        {_, {ca, _}} when ca >= cb -> true
        _ -> false
      end
    end)
  end

  @spec descends_dot(vclock, dot) :: boolean
  def descends_dot(vclock, dot) do
    descends(vclock, [dot])
  end

  @spec pure_dot(dot) :: pure_dot
  def pure_dot({n, {c, _}}) do
    {n, c}
  end

  @doc """
  true if `a` strictly dominates `b`.
  """
  @spec dominates(vclock, vclock) :: boolean
  def dominates(a, b) do
    descends(a, b) && !descends(b, a)
  end

  @doc """
  Combine all vclocks in the input list into their least
  possible common descendant
  """
  @spec merge([vclock]) :: vclock
  def merge([]), do: []
  def merge([single_vclock]), do: [single_vclock]
  def merge([first | res]), do: merge(res, first)

  @spec merge(vclock | [vclock], vclock | [vclock]) :: vclock
  defp merge([], acc), do: acc

  defp merge([h | t], acc) do
    merge(t, merge(h, acc, []))
  end

  @spec merge(vclock | [vclock], vclock | [vclock], vclock | [vclock]) :: vclock
  defp merge([], [], acc), do: Enum.reverse(acc)
  defp merge(rem, [], acc), do: Enum.reverse(acc, rem)
  defp merge([], rem, acc), do: Enum.reverse(acc, rem)

  defp merge(
         lhs = [{lhs_node, {lhs_cnt, lhs_time}} = lhs_vclock | lhs_rem],
         rhs = [{rhs_node, {rhs_cnt, rhs_time}} = rhs_vclock | rhs_rem],
         acc
       ) do
    cond do
      lhs_node < rhs_node ->
        merge(lhs_rem, rhs, [lhs_vclock | acc])

      rhs_node < lhs_node ->
        merge(lhs, rhs_rem, [rhs_vclock | acc])

      true ->
        new_cnt =
          cond do
            lhs_cnt < rhs_cnt -> {rhs_cnt, rhs_time}
            rhs_cnt < lhs_cnt -> {lhs_cnt, lhs_time}
            true -> {lhs_cnt, max(lhs_time, rhs_time)}
          end

        merge(lhs_rem, rhs_rem, [{lhs_node, new_cnt} | acc])
    end
  end

  @doc """
  Get the counter value in a vclock set from node
  """
  @spec get_counter(vclock_node, vclock) :: counter
  def get_counter(node, vclock) do
    case List.keyfind(vclock, node, 0) do
      {_, {cnt, _}} -> cnt
      _ -> 0
    end
  end

  @doc """
  Get the timestamp value in a vclock set from node
  """
  @spec get_timestamp(vclock_node, vclock) :: timestamp | :undefined
  def get_timestamp(node, vclock) do
    case List.keyfind(vclock, node, 0) do
      {_, {_, t}} -> t
      _ -> :undefined
    end
  end

  @doc """
  Get the entry `dot` for `vclock_node` from `vclock`
  """
  @spec get_dot(vclock_node, vclock) :: {:ok, dot} | :undefined
  def get_dot(node, vclock) do
    case List.keyfind(vclock, node, 0) do
      {^node, {_, _}} = dot -> {:ok, dot}
      _ -> :undefined
    end
  end

  @doc """
  Check is the given argument a valid dot
  """
  @spec valid_dot(dot) :: boolean
  def valid_dot({_, {c, t}}) when is_integer(c) and is_integer(t), do: true
  def valid_dot(_), do: false

  @doc """
  Increment vclock at node
  """
  @spec increment(vclock_node, vclock) :: vclock
  def increment(node, vclock) do
    increment(node, timestamp(), vclock)
  end

  @spec increment(vclock_node, timestamp, vclock) :: vclock
  defp increment(node, timestamp, vclock) do
    case List.keytake(vclock, node, 0) do
      {{node, {cnt, t}}, rem} -> [{node, {cnt + 1, t}} | rem]
      nil -> [{node, {1, timestamp}} | vclock]
    end
  end

  @doc """
  Return the list of all nodes that have ever incremented vclock
  """
  @spec all_nodes(vclock) :: [vclock_node]
  def all_nodes(vclock) do
    for {node, _} <- vclock, do: node
  end

  @doc """
  Compares two vclocks for equality
  """
  @spec equal(vclock, vclock) :: boolean
  def equal(v1, v2) do
    List.keysort(v1, 0) === List.keysort(v2, 0)
  end

  @doc """
  Possibly shrink the size of a vclock, depending on current age and size
  """
  @spec prune(vclock, integer, [{atom, integer}]) :: vclock
  def prune(v, now, bucket_props) do
    sort_v =
      Enum.sort(v, fn {n1, {_, t1}}, {n2, {_, t2}} ->
        {t1, n1} < {t2, n2}
      end)

    prune_vclock1(sort_v, now, bucket_props)
  end

  defp prune_vclock1(v, now, props) do
    case length(v) <= get_property(:small_vclock, props) do
      true ->
        v

      false ->
        {_, {_, head_time}} = hd(v)

        case now - head_time < get_property(:young_vclock, props) do
          true -> v
          false -> prune_vclock1(v, now, props, head_time)
        end
    end
  end

  defp prune_vclock1(v, now, props, head_time) do
    case length(v) > get_property(:big_vclock, props) or
           now - head_time > get_property(:old_vclock, props) do
      true -> prune_vclock1(tl(v), now, props)
      false -> v
    end
  end

  defp get_property(key, pair_list) do
    case List.keyfind(pair_list, key, 0) do
      {^key, value} -> value
      nil -> :undefined
    end
  end

  @spec timestamp :: timestamp
  defp timestamp() do
    {mega_seconds, seconds, _} = :os.timestamp()
    mega_seconds * 1000_000 + seconds
  end
end
