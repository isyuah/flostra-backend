from __future__ import annotations

import json
from typing import Any

from aiobotocore.session import get_session
from botocore.config import Config as BotoCoreConfig

from io_utils import FileIoService
from runtime_files import (
    build_file_ref,
    get_file_record,
    is_file_ref,
    new_file_id,
    upsert_file_record,
)
from security.egress import check_outbound_url

from .base import JsonDict, WorkflowNode, register_node


# Helper to create an S3 client
async def _get_s3_client(conn_cfg: dict[str, Any]):
    session = get_session()
    endpoint = conn_cfg.get("endpoint_url")
    if endpoint:
        # 出站网络策略：S3 端点必须通过校验，防止指向内网对象存储。
        check_outbound_url(endpoint)
    config = BotoCoreConfig(
        signature_version="s3v4",
        retries={"max_attempts": 10, "mode": "standard"},
        connect_timeout=10,
        read_timeout=60,
    )
    client_kwargs = {
        "region_name": conn_cfg.get("region_name"),
        "aws_access_key_id": conn_cfg.get("access_key_id"),
        "aws_secret_access_key": conn_cfg.get("secret_access_key"),
        "endpoint_url": conn_cfg.get("endpoint_url"),
        "config": config,
    }
    return session.create_client("s3", **client_kwargs)


@register_node
class S3GetNode(WorkflowNode):
    type = "s3_get"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "S3 获取文件",
            "description": "从 S3 获取文件并返回文件引用",
            "category": "Storage",
            "icon": "cloud-arrow-down",
            "inputs": [
                {
                    "name": "connection",
                    "label": "连接信息",
                    "type": "object",
                    "required": True,
                    "description": "JSON对象: {access_key_id, secret_access_key, region_name, endpoint_url}",
                },
                {"name": "bucket", "label": "Bucket 名称", "type": "string", "required": True},
                {"name": "key", "label": "对象 Key", "type": "string", "required": True},
            ],
            "outputs": [
                {"name": "file", "label": "文件引用", "type": "object", "required": True},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        conn_cfg = inputs.get("connection")
        bucket = inputs.get("bucket")
        key = inputs.get("key")
        
        if not context:
            raise ValueError("S3节点需要运行时上下文")

        if not all([conn_cfg, bucket, key]):
            raise ValueError("连接信息, Bucket, Key 不能为空")

        async with await _get_s3_client(conn_cfg) as client:
            try:
                # Get object metadata first
                response = await client.head_object(Bucket=bucket, Key=key)
                size = response.get("ContentLength")
                mime = response.get("ContentType", "application/octet-stream") # Default MIME type

                # Generate a presigned URL
                # NOTE: This URL is tied to the connection config.
                presigned_url = await client.generate_presigned_url(
                    ClientMethod="get_object",
                    Params={"Bucket": bucket, "Key": key},
                    ExpiresIn=3600,  # URL valid for 1 hour
                )

                file_id = new_file_id()

                # Store S3 specific metadata in the context
                upsert_file_record(
                    context,
                    file_id,
                    name=key.split("/")[-1],
                    mime=mime,
                    size=size,
                    source="s3",
                    # Store full connection config for later access if needed, without revealing secrets
                    meta={
                        "bucket": bucket,
                        "key": key,
                        "region_name": conn_cfg.get("region_name"),
                        "endpoint_url": conn_cfg.get("endpoint_url"),
                    },
                    url=presigned_url, # Store presigned URL
                )

                file_ref = build_file_ref(
                    file_id=file_id,
                    name=key.split("/")[-1],
                    mime=mime,
                    size=size,
                    source="s3",
                )
                return {"file": file_ref}

            except client.exceptions.NoSuchKey:
                raise ValueError(f"S3 对象未找到: s3://{bucket}/{key}")
            except Exception as e:
                raise ValueError(f"S3 获取失败: {e}")


@register_node
class S3PutNode(WorkflowNode):
    type = "s3_put"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "S3 存放文件",
            "description": "上传内容或文件引用到 S3",
            "category": "Storage",
            "icon": "cloud-arrow-up",
            "inputs": [
                {
                    "name": "connection",
                    "label": "连接信息",
                    "type": "object",
                    "required": True,
                    "description": "JSON对象: {access_key_id, secret_access_key, region_name, endpoint_url}",
                },
                {"name": "bucket", "label": "Bucket 名称", "type": "string", "required": True},
                {"name": "key", "label": "对象 Key", "type": "string", "required": True},
                {"name": "content", "label": "内容 (Text/Bytes/JSON)", "type": "any", "required": False},
                {"name": "file", "label": "文件引用", "type": "object", "required": False},
            ],
            "outputs": [
                {"name": "success", "label": "是否成功", "type": "boolean", "required": True},
                {"name": "file", "label": "上传的文件引用", "type": "object", "required": True},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        conn_cfg = inputs.get("connection")
        bucket = inputs.get("bucket")
        key = inputs.get("key")
        content_input = inputs.get("content")
        file_ref_input = inputs.get("file")
        
        if not context:
             raise ValueError("S3节点需要运行时上下文")

        if not all([conn_cfg, bucket, key]):
            raise ValueError("连接信息, Bucket, Key 不能为空")

        if content_input is None and file_ref_input is None:
            raise ValueError("必须提供 'content' 或 'file' 其中之一")
        
        upload_data: bytes | None = None
        mime_type: str | None = "application/octet-stream"
        content_length: int | None = None

        # 1. 尝试从文件引用处理
        if file_ref_input and is_file_ref(file_ref_input):
            file_record = get_file_record(context, file_ref_input.get("id")) or file_ref_input
            
            # S3-to-S3 Copy Optimization
            if file_record.get("source") in ("s3", "external_s3") and file_record.get("meta", {}).get("bucket"):
                src_meta = file_record["meta"]
                src_endpoint = src_meta.get("endpoint_url")
                dst_endpoint = conn_cfg.get("endpoint_url")
                
                if src_endpoint == dst_endpoint: 
                    try:
                        async with await _get_s3_client(conn_cfg) as client:
                            await client.copy_object(
                                Bucket=bucket,
                                Key=key,
                                CopySource={"Bucket": src_meta["bucket"], "Key": src_meta["key"]},
                            )
                        # Success copy
                        file_id = new_file_id()
                        upsert_file_record(
                            context,
                            file_id,
                            name=key.split("/")[-1],
                            mime=file_record.get("mime"),
                            size=file_record.get("size"),
                            source="s3",
                            meta={"bucket": bucket, "key": key, **conn_cfg},
                        )
                        return {
                            "success": True, 
                            "file": build_file_ref(
                                file_id=file_id,
                                name=key.split("/")[-1],
                                mime=file_record.get("mime"),
                                size=file_record.get("size"),
                                source="s3",
                            )
                        }
                    except Exception:
                        pass

            # Generic File Reading via IO Service
            upload_data, _, mime = await FileIoService.get_file_bytes(context, file_ref_input)
            mime_type = mime
            content_length = len(upload_data)

        # 2. 尝试从直接内容处理
        elif content_input is not None:
            if isinstance(content_input, str):
                upload_data = content_input.encode("utf-8")
                mime_type = "text/plain"
            elif isinstance(content_input, bytes):
                upload_data = content_input
                mime_type = "application/octet-stream"
            elif isinstance(content_input, (dict, list)):
                upload_data = json.dumps(content_input, ensure_ascii=False).encode("utf-8")
                mime_type = "application/json"
            else:
                upload_data = str(content_input).encode("utf-8")
                mime_type = "text/plain"
            content_length = len(upload_data)

        if upload_data is None:
            raise ValueError("无法解析上传内容")
        
        async with await _get_s3_client(conn_cfg) as client:
            try:
                await client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=upload_data,
                    ContentType=mime_type,
                    ContentLength=content_length,
                )

                file_id = new_file_id()
                upsert_file_record(
                    context,
                    file_id,
                    name=key.split("/")[-1],
                    mime=mime_type,
                    size=content_length,
                    source="s3",
                    meta={
                        "bucket": bucket,
                        "key": key,
                        "region_name": conn_cfg.get("region_name"),
                        "endpoint_url": conn_cfg.get("endpoint_url"),
                    },
                )

                return {
                    "success": True, 
                    "file": build_file_ref(
                        file_id=file_id,
                        name=key.split("/")[-1],
                        mime=mime_type,
                        size=content_length,
                        source="s3",
                    )
                }
            except Exception as e:
                raise ValueError(f"S3 Put 失败: {e}")


@register_node
class S3ListNode(WorkflowNode):
    type = "s3_list"

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "S3 列出文件",
            "description": "列出 S3 Bucket 中的对象",
            "category": "Storage",
            "icon": "view-list",
            "inputs": [
                {
                    "name": "connection",
                    "label": "连接信息",
                    "type": "object",
                    "required": True,
                    "description": "JSON对象: {access_key_id, secret_access_key, region_name, endpoint_url}",
                },
                {"name": "bucket", "label": "Bucket 名称", "type": "string", "required": True},
                {"name": "prefix", "label": "前缀 (Prefix)", "type": "string", "required": False},
            ],
            "outputs": [
                {"name": "files", "label": "文件引用列表", "type": "array", "required": True},
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        conn_cfg = inputs.get("connection")
        bucket = inputs.get("bucket")
        prefix = inputs.get("prefix")
        
        if not context:
             raise ValueError("S3节点需要运行时上下文")

        if not all([conn_cfg, bucket]):
            raise ValueError("连接信息, Bucket 不能为空")

        files_refs: list[JsonDict] = []

        async with await _get_s3_client(conn_cfg) as client:
            try:
                paginator = client.get_paginator("list_objects_v2")
                async for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                    for obj in page.get("Contents", []):
                        key = obj["Key"]
                        file_id = new_file_id()

                        # Store S3 specific metadata in the context
                        upsert_file_record(
                            context,
                            file_id,
                            name=key.split("/")[-1],
                            mime="application/octet-stream", # Default; could try to infer or HEAD each for better info
                            size=obj.get("Size"),
                            source="s3",
                            meta={
                                "bucket": bucket,
                                "key": key,
                                "region_name": conn_cfg.get("region_name"),
                                "endpoint_url": conn_cfg.get("endpoint_url"),
                            },
                        )

                        file_ref = build_file_ref(
                            file_id=file_id,
                            name=key.split("/")[-1],
                            mime="application/octet-stream",
                            size=obj.get("Size"),
                            source="s3",
                        )
                        files_refs.append(file_ref)
                
                return {"files": files_refs}

            except Exception as e:
                raise ValueError(f"S3 List 失败: {e}")