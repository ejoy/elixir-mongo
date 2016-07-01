Code.require_file "test_helper.exs", __DIR__

defmodule Mongo.Test do
  use ExUnit.Case, async: false

  setup do
    mongo = Mongo.connect!
    db = Mongo.db(mongo, "test")
    anycoll4 = Mongo.Db.collection(db, "coll_aggr")
    Mongo.Collection.drop anycoll4
    [
        %{a: 0, value: 0},
        %{a: 1, value: 1},
        %{a: 2, value: 1},
        %{a: 3, value: 1},
        %{a: 4, value: 1},
        %{a: 5, value: 3} ] |> Mongo.Collection.insert(anycoll4)
    { :ok, mongo: mongo, db: db, anycoll4: anycoll4 }
  end
  doctest Mongo
  doctest Mongo.Server
  doctest Mongo.Collection

end
