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
from typing import Any

from pymilvus import MilvusClient

from airflow.hooks.base import BaseHook


class MilvusHook(BaseHook):
    """
    Hook for interfacing with a Milvus instance.

    """

    default_conn_name = "milvus_default"

    # todo 这部分还不知道是在哪里的
    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "url": StringField(
                lazy_gettext("URL"),
                widget=BS3TextFieldWidget(),
                description="XXXX."
                "Example: XXXXXX",
            )
        }

    # todo 这里和页面进行互动 可以等在页面上验证时再来进行修正
    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema"],
            "relabeling": {},
            "placeholders": {},
        }

    def __init__(self, conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def get_conn(self) -> MilvusClient:
        connection = self.get_connection(self.conn_id)

        uri = connection.get_uri()
        user = connection.login or None
        password = connection.password  or None

        extra = connection.extra_dejson
        db_name = extra.get("db_name", None)
        token = extra.get("token", None)
        timeout = extra.get("token", None)

        client = MilvusClient(
            uri=uri,
            user=user,
            password=password,
            db_name=db_name,
            token=token,
            timeout=timeout,
        )
        client.search()
        return client

    @cached_property
    def conn(self) -> MilvusClient:
        return self.get_conn()

