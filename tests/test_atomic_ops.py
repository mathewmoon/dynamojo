import asyncio

import pytest
from unittest.mock import MagicMock

from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig, JoinedAttribute
from dynamojo.index import IndexList, IndexMap, TableIndex


_table_index = TableIndex(name="table", partitionkey="PK", sortkey="SK")
_indexes = IndexList(_table_index)


def make_item():
    """Return a fresh (model instance, mock client) pair for each test."""
    mock_client = MagicMock()
    config = DynamojoConfig(
        indexes=_indexes,
        index_maps=[
            IndexMap(index=_table_index, partitionkey="entity_id", sortkey="sort_key")
        ],
        joined_attributes=[],
        table="test-table",
        dynamo_client=mock_client,
    )

    class TestItem(DynamojoBase):
        entity_id: str
        sort_key: str
        keys: list[str]
        count: int
        score: float
        metadata: dict
        name: str

        @classmethod
        def _config(cls):
            return config

    instance = TestItem(
        entity_id="eid-1",
        sort_key="sk-1",
        keys=["a", "b", "c"],
        count=5,
        score=1.5,
        metadata={"x": 1, "y": 2},
        name="Alice",
    )
    return instance, mock_client


def run(coro):
    return asyncio.run(coro)


def call_kwargs(mock_client):
    return mock_client.update_item.call_args.kwargs


# ---------------------------------------------------------------------------
# list_append
# ---------------------------------------------------------------------------

class TestListAppend:
    def test_expression(self):
        item, client = make_item()
        run(item.list_append("keys", ["d", "e"]))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "SET #field = list_append(#field, :items)"
        assert kw["ExpressionAttributeNames"] == {"#field": "keys"}
        assert ":items" in kw["ExpressionAttributeValues"]

    def test_local_state(self):
        item, _ = make_item()
        run(item.list_append("keys", ["d"]))
        assert item.keys == ["a", "b", "c", "d"]

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.list_append("keys", ["d"]))
        assert "keys" not in item._diff.keys

    def test_table_name_and_key_present(self):
        item, client = make_item()
        run(item.list_append("keys", ["d"]))
        kw = call_kwargs(client)
        assert kw["TableName"] == "test-table"
        assert "PK" in kw["Key"]
        assert "SK" in kw["Key"]


# ---------------------------------------------------------------------------
# list_prepend
# ---------------------------------------------------------------------------

class TestListPrepend:
    def test_expression(self):
        item, client = make_item()
        run(item.list_prepend("keys", ["z"]))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "SET #field = list_append(:items, #field)"

    def test_local_state(self):
        item, _ = make_item()
        run(item.list_prepend("keys", ["z"]))
        assert item.keys == ["z", "a", "b", "c"]

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.list_prepend("keys", ["z"]))
        assert "keys" not in item._diff.keys


# ---------------------------------------------------------------------------
# list_remove
# ---------------------------------------------------------------------------

class TestListRemove:
    def test_expression(self):
        item, client = make_item()
        run(item.list_remove("keys", 1))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "REMOVE #field[1]"

    def test_no_expression_attribute_values_without_condition(self):
        item, client = make_item()
        run(item.list_remove("keys", 0))
        kw = call_kwargs(client)
        assert "ExpressionAttributeValues" not in kw

    def test_local_state(self):
        item, _ = make_item()
        run(item.list_remove("keys", 1))
        assert item.keys == ["a", "c"]

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.list_remove("keys", 0))
        assert "keys" not in item._diff.keys


# ---------------------------------------------------------------------------
# list_set
# ---------------------------------------------------------------------------

class TestListSet:
    def test_expression(self):
        item, client = make_item()
        run(item.list_set("keys", 0, "z"))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "SET #field[0] = :value"
        assert kw["ExpressionAttributeNames"] == {"#field": "keys"}

    def test_local_state(self):
        item, _ = make_item()
        run(item.list_set("keys", 0, "z"))
        assert item.keys == ["z", "b", "c"]

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.list_set("keys", 2, "z"))
        assert "keys" not in item._diff.keys


# ---------------------------------------------------------------------------
# number_add
# ---------------------------------------------------------------------------

class TestNumberAdd:
    def test_expression(self):
        item, client = make_item()
        run(item.number_add("count", 3))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "ADD #field :delta"
        assert kw["ExpressionAttributeNames"] == {"#field": "count"}

    def test_local_state_increment(self):
        item, _ = make_item()
        run(item.number_add("count", 3))
        assert item.count == 8

    def test_local_state_decrement(self):
        item, _ = make_item()
        run(item.number_add("count", -2))
        assert item.count == 3

    def test_float_field(self):
        from decimal import Decimal
        item, client = make_item()
        run(item.number_add("score", 0.5))
        # local state uses float arithmetic
        assert item.score == pytest.approx(2.0)
        # wire encoding must use Decimal (boto3 rejects float)
        kw = call_kwargs(client)
        assert kw["ExpressionAttributeValues"][":delta"] == {"N": "0.5"}

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.number_add("count", 1))
        assert "count" not in item._diff.keys


# ---------------------------------------------------------------------------
# dict_set
# ---------------------------------------------------------------------------

class TestDictSet:
    def test_expression(self):
        item, client = make_item()
        run(item.dict_set("metadata", "z", "val"))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "SET #field.#key = :value"
        assert kw["ExpressionAttributeNames"] == {"#field": "metadata", "#key": "z"}
        assert ":value" in kw["ExpressionAttributeValues"]

    def test_local_state_adds_key(self):
        item, _ = make_item()
        run(item.dict_set("metadata", "z", "val"))
        assert item.metadata == {"x": 1, "y": 2, "z": "val"}

    def test_local_state_updates_key(self):
        item, _ = make_item()
        run(item.dict_set("metadata", "x", 99))
        assert item.metadata["x"] == 99

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.dict_set("metadata", "z", 1))
        assert "metadata" not in item._diff.keys


# ---------------------------------------------------------------------------
# dict_remove
# ---------------------------------------------------------------------------

class TestDictRemove:
    def test_expression(self):
        item, client = make_item()
        run(item.dict_remove("metadata", "x"))
        kw = call_kwargs(client)
        assert kw["UpdateExpression"] == "REMOVE #field.#key"
        assert kw["ExpressionAttributeNames"] == {"#field": "metadata", "#key": "x"}

    def test_no_expression_attribute_values_without_condition(self):
        item, client = make_item()
        run(item.dict_remove("metadata", "x"))
        kw = call_kwargs(client)
        assert "ExpressionAttributeValues" not in kw

    def test_local_state(self):
        item, _ = make_item()
        run(item.dict_remove("metadata", "x"))
        assert item.metadata == {"y": 2}

    def test_clears_diff(self):
        item, _ = make_item()
        run(item.dict_remove("metadata", "x"))
        assert "metadata" not in item._diff.keys


# ---------------------------------------------------------------------------
# set_if_not_exists
# ---------------------------------------------------------------------------

class TestSetIfNotExists:
    def test_expression(self):
        item, client = make_item()
        run(item.set_if_not_exists("name", "Default"))
        kw = call_kwargs(client)
        assert "if_not_exists" in kw["UpdateExpression"]
        assert kw["ExpressionAttributeNames"] == {"#field": "name"}

    def test_local_state(self):
        item, _ = make_item()
        run(item.set_if_not_exists("name", "Default"))
        assert item.name == "Default"

    def test_clears_diff(self):
        item, _ = make_item()
        item.name = "Bob"
        run(item.set_if_not_exists("name", "Default"))
        assert "name" not in item._diff.keys

    def test_works_on_any_field_type(self):
        item, client = make_item()
        run(item.set_if_not_exists("count", 0))
        kw = call_kwargs(client)
        assert "if_not_exists" in kw["UpdateExpression"]


# ---------------------------------------------------------------------------
# ConditionExpression handling
# ---------------------------------------------------------------------------

class TestConditionExpression:
    def test_condition_merged_into_list_append(self):
        from boto3.dynamodb.conditions import Attr
        item, client = make_item()
        run(item.list_append("keys", ["d"], ConditionExpression=Attr("name").eq("Alice")))
        kw = call_kwargs(client)
        assert "ConditionExpression" in kw
        assert "ExpressionAttributeValues" in kw

    def test_list_remove_with_condition_includes_expression_attribute_values(self):
        from boto3.dynamodb.conditions import Attr
        item, client = make_item()
        run(item.list_remove("keys", 0, ConditionExpression=Attr("count").eq(5)))
        kw = call_kwargs(client)
        assert "ConditionExpression" in kw
        assert "ExpressionAttributeValues" in kw

    def test_dict_remove_with_condition_includes_expression_attribute_values(self):
        from boto3.dynamodb.conditions import Attr
        item, client = make_item()
        run(item.dict_remove("metadata", "x", ConditionExpression=Attr("count").gt(0)))
        kw = call_kwargs(client)
        assert "ConditionExpression" in kw
        assert "ExpressionAttributeValues" in kw


# ---------------------------------------------------------------------------
# Type validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_nonexistent_field_raises_attribute_error(self):
        item, _ = make_item()
        with pytest.raises(AttributeError, match="does not exist"):
            run(item.list_append("nonexistent", ["x"]))

    def test_number_add_on_list_raises_type_error(self):
        item, _ = make_item()
        with pytest.raises(TypeError):
            run(item.number_add("keys", 1))

    def test_list_append_on_int_raises_type_error(self):
        item, _ = make_item()
        with pytest.raises(TypeError):
            run(item.list_append("count", [1]))

    def test_dict_set_on_list_raises_type_error(self):
        item, _ = make_item()
        with pytest.raises(TypeError):
            run(item.dict_set("keys", "x", 1))

    def test_dict_remove_on_int_raises_type_error(self):
        item, _ = make_item()
        with pytest.raises(TypeError):
            run(item.dict_remove("count", "x"))


# ---------------------------------------------------------------------------
# Diff isolation — only the operated field is synced
# ---------------------------------------------------------------------------

class TestDiffIsolation:
    def test_other_dirty_fields_preserved_after_list_append(self):
        item, _ = make_item()
        item.name = "Bob"
        assert "name" in item._diff.keys
        run(item.list_append("keys", ["d"]))
        assert "name" in item._diff.keys
        assert "keys" not in item._diff.keys

    def test_other_dirty_fields_preserved_after_number_add(self):
        item, _ = make_item()
        item.name = "Bob"
        run(item.number_add("count", 1))
        assert "name" in item._diff.keys
        assert "count" not in item._diff.keys

    def test_other_dirty_fields_preserved_after_dict_set(self):
        item, _ = make_item()
        item.name = "Bob"
        run(item.dict_set("metadata", "z", 1))
        assert "name" in item._diff.keys
        assert "metadata" not in item._diff.keys
