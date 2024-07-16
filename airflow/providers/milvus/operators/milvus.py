# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence, Union, Dict, List, Optional

from airflow.models import BaseOperator
from airflow.providers.milvus.hooks.milvus import MilvusHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MilvusIngestOperator(BaseOperator):
    """
    Insert data into the milvus collection.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MilvusIngestOperator`

    If the Milvus Client was initiated without an existing Collection, the first dict passed
    in will be used to initiate the collection.

    :param collection_name: The name of an existing collection.
    :param data: The data to insert into the current collection.
        A list of dicts to pass in. If list not provided, will cast to list.
    :param timeout: The timeout to use, will override init timeout. Defaults to None.
    :param partition_name: The name of a partition in the current collection.
        If specified, the data is to be inserted into the specified partition.

    :return: Number of rows that were inserted and the inserted primary key list.
    """

    template_fields: Sequence[str] = (
        "collection_name",
        "data",
        "timeout",
        "partition_name",
    )

    def __init__(
        self,
        *,
        conn_id: str = MilvusHook.default_conn_name,
        collection_name: str,
        data: Union[Dict, List[Dict]],
        timeout: Optional[float] = None,
        partition_name: Optional[str] = "",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

        self.collection_name = collection_name
        self.data = data
        self.timeout = timeout
        self.partition_name = partition_name

    @cached_property
    def hook(self) -> MilvusHook:
        """Return an instance of MilvusHook."""
        return MilvusHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> None:
        """Insert points to a Milvus collection."""
        self.hook.conn.insert(
            collection_name=self.collection_name,
            data=self.data,
            timeout=self.timeout,
            partition_name=self.partition_name,
        )
