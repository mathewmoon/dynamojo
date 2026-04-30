import asyncio

import pytest
from unittest.mock import MagicMock

from boto3.dynamodb.conditions import Attr

from dynamojo.base import DynamojoBase
from dynamojo.config import DynamojoConfig
from dynamojo.index import IndexList, IndexMap, TableIndex


# ---------------------------------------------------------------------------
# Composite-key model (PK + SK)
# ---------------------------------------------------------------------------

_composite_index = TableIndex(name="table", partitionkey="PK", sortkey="SK")
_composite_indexes = IndexList(_composite_index)


def make_composite_model():
    mock_client = MagicMock()
    mock_client.update_item.return_value = {"Attributes": {}}
    mock_client.delete_item.return_value = {}

    config = DynamojoConfig(
        indexes=_composite_indexes,
        index_maps=[
            IndexMap(index=_composite_index, partitionkey="entity_id", sortkey="sort_key")
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

    return TestItem, mock_client


def run(coro):
    return asyncio.run(coro)


def update_kwargs(client):
    return client.update_item.call_args.kwargs


def delete_kwargs(client):
    return client.delete_item.call_args.kwargs


# ===========================================================================
# update_by_key
# ===========================================================================


class TestUpdateByKeyBasics:
    def test_no_get_item_call(self):
        """The whole point: no upfront fetch."""
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", number_add={"count": 1}))
        client.get_item.assert_not_called()

    def test_returns_update_item_response(self):
        Model, client = make_composite_model()
        client.update_item.return_value = {"Attributes": {"sentinel": True}}
        result = run(Model.update_by_key("eid-1", "sk-1", number_add={"count": 1}))
        assert result == {"Attributes": {"sentinel": True}}

    def test_table_name_present(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", number_add={"count": 1}))
        assert update_kwargs(client)["TableName"] == "test-table"

    def test_key_serialized_for_composite(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", number_add={"count": 1}))
        kw = update_kwargs(client)
        assert kw["Key"] == {"PK": {"S": "eid-1"}, "SK": {"S": "sk-1"}}

    def test_requires_at_least_one_atomic_op(self):
        Model, client = make_composite_model()
        with pytest.raises(ValueError, match="at least one atomic-op kwarg"):
            run(Model.update_by_key("eid-1", "sk-1"))
        client.update_item.assert_not_called()

    def test_passthrough_opts(self):
        Model, client = make_composite_model()
        run(Model.update_by_key(
            "eid-1", "sk-1",
            number_add={"count": 1},
            ReturnValues="ALL_NEW",
        ))
        assert update_kwargs(client)["ReturnValues"] == "ALL_NEW"


class TestUpdateByKeyOps:
    def test_list_append(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", list_append={"keys": ["d"]}))
        kw = update_kwargs(client)
        assert "list_append(#keys, :_la_keys)" in kw["UpdateExpression"]
        assert kw["ExpressionAttributeNames"]["#keys"] == "keys"

    def test_list_prepend(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", list_prepend={"keys": ["z"]}))
        kw = update_kwargs(client)
        assert "list_append(:_lp_keys, #keys)" in kw["UpdateExpression"]

    def test_list_remove(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", list_remove={"keys": 0}))
        kw = update_kwargs(client)
        assert kw["UpdateExpression"].strip() == "REMOVE #keys[0]"
        assert "ExpressionAttributeValues" not in kw

    def test_list_set(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", list_set={"keys": (1, "X")}))
        kw = update_kwargs(client)
        assert "#keys[1] = :_ls_keys" in kw["UpdateExpression"]

    def test_number_add(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", number_add={"count": 3}))
        kw = update_kwargs(client)
        assert "ADD #count :_na_count" in kw["UpdateExpression"]
        assert kw["ExpressionAttributeValues"][":_na_count"] == {"N": "3"}

    def test_number_add_float_uses_decimal(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", number_add={"score": 0.5}))
        kw = update_kwargs(client)
        assert kw["ExpressionAttributeValues"][":_na_score"] == {"N": "0.5"}

    def test_dict_set_shallow(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", dict_set={"metadata.x": 1}))
        kw = update_kwargs(client)
        assert "#metadata.#metadata__x = :_ds_metadata__x" in kw["UpdateExpression"]
        assert kw["ExpressionAttributeValues"][":_ds_metadata__x"] == {"N": "1"}

    def test_dict_set_deep(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", dict_set={"metadata.a.b.c": "deep"}))
        kw = update_kwargs(client)
        assert (
            "#metadata.#metadata__a.#metadata__a__b.#metadata__a__b__c = :_ds_metadata__a__b__c"
            in kw["UpdateExpression"]
        )

    def test_dict_set_requires_path(self):
        Model, _ = make_composite_model()
        with pytest.raises(ValueError, match="must include a dot-separated path"):
            run(Model.update_by_key("eid-1", "sk-1", dict_set={"metadata": "bad"}))

    def test_dict_remove_shallow(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", dict_remove=["metadata.x"]))
        kw = update_kwargs(client)
        assert kw["UpdateExpression"].strip() == "REMOVE #metadata.#metadata__x"

    def test_dict_remove_requires_path(self):
        Model, _ = make_composite_model()
        with pytest.raises(ValueError, match="must include a dot-separated path"):
            run(Model.update_by_key("eid-1", "sk-1", dict_remove=["metadata"]))

    def test_set_if_not_exists(self):
        Model, client = make_composite_model()
        run(Model.update_by_key("eid-1", "sk-1", set_if_not_exists={"name": "Default"}))
        kw = update_kwargs(client)
        assert "if_not_exists(#name, :_ine_name)" in kw["UpdateExpression"]


class TestUpdateByKeyComposite:
    def test_set_remove_and_add_in_one_expression(self):
        Model, client = make_composite_model()
        run(Model.update_by_key(
            "eid-1", "sk-1",
            list_append={"keys": ["d"]},   # SET
            list_remove={"keys": 0},        # REMOVE
            number_add={"count": 1},        # ADD
        ))
        expr = update_kwargs(client)["UpdateExpression"]
        assert "SET" in expr
        assert "REMOVE" in expr
        assert "ADD" in expr

    def test_single_round_trip(self):
        Model, client = make_composite_model()
        run(Model.update_by_key(
            "eid-1", "sk-1",
            list_append={"keys": ["d"]},
            number_add={"count": 1},
            dict_set={"metadata.x": 99},
        ))
        assert client.update_item.call_count == 1


class TestUpdateByKeyConditionExpression:
    def test_condition_merges(self):
        Model, client = make_composite_model()
        run(Model.update_by_key(
            "eid-1", "sk-1",
            number_add={"count": 1},
            ConditionExpression=Attr("name").eq("Alice"),
        ))
        kw = update_kwargs(client)
        assert "ConditionExpression" in kw
        assert kw["ExpressionAttributeValues"]
        # Both atomic-op values and condition-op values present
        assert any(k.startswith(":_na_") for k in kw["ExpressionAttributeValues"])

    def test_condition_with_in_clause(self):
        """Mirrors the Sandbox.extend_ttl shape: status IN (CREATING, ACTIVE)."""
        Model, client = make_composite_model()
        run(Model.update_by_key(
            "eid-1", "sk-1",
            number_add={"count": 1},
            ConditionExpression=Attr("name").is_in(["A", "B"]),
        ))
        kw = update_kwargs(client)
        assert "ConditionExpression" in kw

    def test_condition_without_atomic_ops_still_requires_op(self):
        Model, _ = make_composite_model()
        with pytest.raises(ValueError):
            run(Model.update_by_key(
                "eid-1", "sk-1",
                ConditionExpression=Attr("name").eq("Alice"),
            ))


class TestUpdateByKeyKeyValidation:
    def test_composite_table_requires_sk(self):
        Model, _ = make_composite_model()
        with pytest.raises(ValueError, match="sk argument is required"):
            run(Model.update_by_key("eid-1", number_add={"count": 1}))


# ===========================================================================
# delete_by_key
# ===========================================================================


class TestDeleteByKeyBasics:
    def test_no_get_item_call(self):
        Model, client = make_composite_model()
        run(Model.delete_by_key("eid-1", "sk-1"))
        client.get_item.assert_not_called()

    def test_returns_delete_item_response(self):
        Model, client = make_composite_model()
        client.delete_item.return_value = {"Attributes": {"sentinel": True}}
        result = run(Model.delete_by_key("eid-1", "sk-1"))
        assert result == {"Attributes": {"sentinel": True}}

    def test_table_name_and_key(self):
        Model, client = make_composite_model()
        run(Model.delete_by_key("eid-1", "sk-1"))
        kw = delete_kwargs(client)
        assert kw["TableName"] == "test-table"
        assert kw["Key"] == {"PK": {"S": "eid-1"}, "SK": {"S": "sk-1"}}

    def test_passthrough_opts(self):
        Model, client = make_composite_model()
        run(Model.delete_by_key("eid-1", "sk-1", ReturnValues="ALL_OLD"))
        assert delete_kwargs(client)["ReturnValues"] == "ALL_OLD"


class TestDeleteByKeyConditionExpression:
    def test_condition_attached(self):
        Model, client = make_composite_model()
        run(Model.delete_by_key(
            "eid-1", "sk-1",
            ConditionExpression=Attr("name").eq("Alice"),
        ))
        kw = delete_kwargs(client)
        assert "ConditionExpression" in kw
        assert "ExpressionAttributeNames" in kw
        assert "ExpressionAttributeValues" in kw


class TestDeleteByKeyKeyValidation:
    def test_composite_table_requires_sk(self):
        Model, _ = make_composite_model()
        with pytest.raises(ValueError, match="sk argument is required"):
            run(Model.delete_by_key("eid-1"))
