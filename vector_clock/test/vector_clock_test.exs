defmodule VectorClockTest do
  use ExUnit.Case

  test "example test" do
    a = VectorClock.fresh()
    b = VectorClock.fresh()
    a1 = VectorClock.increment(:a, a)
    b1 = VectorClock.increment(:b, b)

    true = VectorClock.descends(a1, a)
    true = VectorClock.descends(b1, b)
    false = VectorClock.descends(a1, b1)
    a2 = VectorClock.increment(:a, a1)
    c = VectorClock.merge([a2, b1])
    c1 = VectorClock.increment(:c, c)
    true = VectorClock.descends(c1, a2)
    true = VectorClock.descends(c1, b1)
    false = VectorClock.descends(b1, c1)
    false = VectorClock.descends(b1, a1)
  end

  test "merge test" do
    vc1 = [{"1", {1, 1}}, {"2", {2, 2}}, {"4", {4, 4}}]
    vc2 = [{"3", {3, 3}}, {"4", {3, 3}}]
    assert VectorClock.merge(VectorClock.fresh()) === []

    assert VectorClock.merge([vc1, vc2]) ===
             [{"1", {1, 1}}, {"2", {2, 2}}, {"3", {3, 3}}, {"4", {4, 4}}]
  end

  test "merge less left test" do
    vc1 = [{"5", {5, 5}}]
    vc2 = [{"6", {6, 6}}, {"7", {7, 7}}]

    assert VectorClock.merge([vc1, vc2]) ===
             [{"5", {5, 5}}, {"6", {6, 6}}, {"7", {7, 7}}]
  end

  test "merge less right test" do
    vc1 = [{"6", {6, 6}}, {"7", {7, 7}}]
    vc2 = [{"5", {5, 5}}]

    assert VectorClock.merge([vc1, vc2]) ===
             [{"5", {5, 5}}, {"6", {6, 6}}, {"7", {7, 7}}]
  end

  test "merge same id test" do
    vc1 = [{"1", {1, 2}}, {"2", {1, 4}}]
    vc2 = [{"1", {1, 3}}, {"3", {1, 5}}]

    assert VectorClock.merge([vc1, vc2]) ===
             [{"1", {1, 3}}, {"2", {1, 4}}, {"3", {1, 5}}]
  end

  test "get entry test" do
    vc = VectorClock.fresh()

    vc1 =
      VectorClock.increment(
        :a,
        VectorClock.increment(
          :c,
          VectorClock.increment(
            :b,
            VectorClock.increment(:a, vc)
          )
        )
      )

    assert {:ok, {:a, {2, _}}} = VectorClock.get_dot(:a, vc1)
    assert {:ok, {:b, {1, _}}} = VectorClock.get_dot(:b, vc1)
    assert {:ok, {:c, {1, _}}} = VectorClock.get_dot(:c, vc1)
    assert :undefined = VectorClock.get_dot(:d, vc1)
  end

  test "valid entry test" do
    vc = VectorClock.fresh()
    vc1 = VectorClock.increment(:c, VectorClock.increment(:b, VectorClock.increment(:a, vc)))

    Enum.each([:a, :b, :c], fn node ->
      assert {:ok, _} = VectorClock.get_dot(node, vc1)
    end)

    assert not VectorClock.valid_dot(:undefined)
    assert not VectorClock.valid_dot("huffle-puff")
    assert not VectorClock.valid_dot([])
  end

  test "prune small test" do
    # vclock with less entries than :small_vclock with be untouched
    now = DateTime.to_unix(DateTime.utc_now())
    old_time = now - 32_000_000
    small_vc = [{"1", {1, old_time}}, {"2", {2, old_time}}, {"3", {3, old_time}}]
    props = [{:small_vclock, 4}]
    assert Enum.sort(small_vc) === Enum.sort(VectorClock.prune(small_vc, now, props))
  end

  test "prune young test" do
    # vclock with all entries younger than :young_vclock with be untouched
    now = DateTime.to_unix(DateTime.utc_now())
    new_time = now - 1
    vc = [{"1", {1, new_time}}, {"2", {2, new_time}}, {"3", {3, new_time}}]
    props = [small_vclock: 1, young_clock: 1000]
    assert Enum.sort(vc) === Enum.sort(VectorClock.prune(vc, now, props))
  end

  test "prune big test" do
    # vclock not preserved by small or young will be pruned down to
    # no larger than :big_vclock entries
    now = DateTime.to_unix(DateTime.utc_now())
    new_time = now - 1000
    vc = [{"1", {1, new_time}}, {"2", {2, new_time}}, {"3", {3, new_time}}]
    props = [small_vclock: 1, young_vclock: 1, big_vclock: 2, old_vclock: 100_000]
    assert length(VectorClock.prune(vc, now, props)) == 2
  end

  test "prune old test" do
    # vclock not preserved by small or young will be pruned down to
    # no larger than big_vclock and no entries more than old_vclock age
    now = DateTime.to_unix(DateTime.utc_now())
    new_time = now - 1000
    old_time = now - 1000_000
    vc = [{"1", {1, new_time}}, {"2", {2, old_time}}, {"3", {3, old_time}}]
    props = [small_vclock: 1, young_vclock: 1, big_vclock: 2, old_vclock: 10_000]
    assert length(VectorClock.prune(vc, now, props)) == 1
  end

  test "prune order test" do
    # vclock with two nodes of the same timestamp with be pruned down
    # to the same node
    now = DateTime.to_unix(DateTime.utc_now())
    old_time = now - 100_000
    vc1 = [{"1", {1, old_time}}, {"2", {2, old_time}}]
    vc2 = Enum.reverse(vc1)
    props = [small_vclock: 1, young_vclock: 1, big_vclock: 2, old_vclock: 10_000]
    assert VectorClock.prune(vc1, now, props) === VectorClock.prune(vc2, now, props)
  end

  test "accesor test" do
    vc = [{"1", {1, 1}}, {"2", {2, 2}}]
    assert 1 = VectorClock.get_counter("1", vc)
    assert 1 = VectorClock.get_timestamp("1", vc)
    assert 2 = VectorClock.get_counter("2", vc)
    assert 2 = VectorClock.get_timestamp("2", vc)
    assert 0 = VectorClock.get_counter("3", vc)
    assert :undefined = VectorClock.get_timestamp("3", vc)
    assert ["1", "2"] = VectorClock.all_nodes(vc)
  end
end
