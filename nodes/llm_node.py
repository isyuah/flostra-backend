from __future__ import annotations

import inspect
import json
import os
from typing import Any

from openai import AsyncOpenAI

from llm_plugins import LLMContext, PluginExecutor

from .base import JsonDict, WorkflowNode, register_node

# 平台模型预设列表（仅存元数据，不存明文密钥）
PRESET_MODELS: dict[str, dict[str, Any]] = {
    "devstral2": {
        "label": "Devstral2",
        "base_url": "https://api.mistral.ai/v1",
        "model": "devstral-2512",
        "api_key_env": "M_API_KEY",
    },
    "deepseek_sf_r1_qwen7b": {
        "label": "DeepSeek R1 Qwen-7B @ SiliconFlow",
        "base_url": "https://api.siliconflow.cn/v1",
        "model": "deepseek-ai/DeepSeek-R1-Distill-Qwen-7B",
        "api_key_env": "LLM_API_KEY",
    },
    "glm4_sf_9b_0414": {
        "label": "GLM-4-9B (2024-04-14) @ SiliconFlow",
        "base_url": "https://api.siliconflow.cn/v1",
        "model": "THUDM/GLM-4-9B-0414",
        "api_key_env": "LLM_API_KEY",
    },
}


def _create_openai_client(base_url: str | None = None, api_key: str | None = None) -> AsyncOpenAI:
    resolved_api_key = api_key or os.getenv("LLM_API_KEY")
    if not resolved_api_key:
        raise RuntimeError("缺少大模型 API Key（可在模型配置中填写，或配置环境变量 LLM_API_KEY）")

    return AsyncOpenAI(api_key=resolved_api_key, base_url=base_url or os.getenv("LLM_BASE_URL") or "https://api.siliconflow.cn/v1")


@register_node
class SuperLLMNode(WorkflowNode):
    """
    大模型对话节点：支持插件链与工具调用
    """

    type = "llm"

    @staticmethod
    def _build_model_picker_extra() -> dict[str, Any]:
        preset_options = [{"label": meta["label"], "value": preset_id} for preset_id, meta in PRESET_MODELS.items()]
        default_preset = (
            next((opt["value"] for opt in preset_options if opt["value"] == "devstral2"), None)
            or (preset_options[0]["value"] if preset_options else "")
        )
        return {
            "placeholder": "选择平台模型或自定义模型",
            "objectSchema": [
                {
                    "name": "source",
                    "label": "模型来源",
                    "type": "string",
                    "widget": "select",
                    "default": "platform",
                    "extra": {"options": [{"label": "平台模型", "value": "platform"}, {"label": "自定义", "value": "custom"}]},
                },
                {
                    "name": "preset",
                    "label": "平台模型",
                    "type": "string",
                    "widget": "select",
                    "desc": "平台可用模型列表（后端提供）",
                    "extra": {"options": preset_options},
                    "default": default_preset,
                },
                {"name": "base_url", "label": "Base URL", "type": "string", "widget": "input"},
                {"name": "api_key", "label": "API Key", "type": "string", "widget": "secret"},
                {"name": "model", "label": "模型标识", "type": "string", "widget": "input"},
                {"name": "supports_vision", "label": "支持视觉输入", "type": "boolean", "widget": "switch", "default": False},
                {"name": "supports_audio", "label": "支持音频输入", "type": "boolean", "widget": "switch", "default": False},
            ],
        }

    @classmethod
    def get_schema(cls) -> JsonDict:
        return {
            "type": cls.type,
            "label": "大模型",
            "description": "调用大模型，支持插件与工具调用；未启用工具时可作为普通对话使用",
            "category": "AI",
            "icon": "llm",
            "inputs": [
                {"name": "message", "label": "消息", "type": "string", "widget": "textarea", "required": True},
                {"name": "session_id", "label": "会话 ID", "type": "string", "required": False},
                {"name": "attachments", "label": "附件", "type": "array", "required": False},
            ],
            "outputs": [
                {"name": "text", "label": "文本输出", "type": "string", "required": True},
                {"name": "raw", "label": "原始响应", "type": "object", "required": False},
            ],
            "parameters": [
                {"name": "model_config", "label": "模型配置", "type": "object", "widget": "model-picker", "required": False, "extra": cls._build_model_picker_extra()},
                {"name": "temperature", "label": "温度", "type": "number", "widget": "slider", "extra": {"min": 0.0, "max": 1.0, "step": 0.1}, "default": 0.7},
                {"name": "system_prompt", "label": "系统提示词（可选）", "type": "string", "widget": "textarea", "required": False},
                {"name": "plugins", "label": "插件列表", "type": "array", "widget": "plugin-list", "required": False},
                {
                    "name": "tools",
                    "label": "工具调用",
                    "type": "array",
                    "widget": "json",
                    "required": False,
                    "desc": "OpenAI tools 数组；若为空则不启用工具调用",
                },
            ],
        }

    @classmethod
    async def run(cls, inputs: JsonDict, params: JsonDict, context: JsonDict = None) -> JsonDict:
        message = inputs.get("message")
        if message is None:
            raise ValueError("LLM 节点缺少必需的输入 message")

        session_id = inputs.get("session_id")
        attachments = inputs.get("attachments") or []

        model_config = params.get("model_config") or {}
        temperature = float(params.get("temperature", 0.7))
        system_prompt = params.get("system_prompt") or ""
        plugins_cfg = params.get("plugins") or []
        tools = params.get("tools") or None

        model_source = (model_config.get("source") or "platform").lower()
        if model_source == "platform":
            preset_id = model_config.get("preset")
            if preset_id and preset_id in PRESET_MODELS:
                preset_meta = PRESET_MODELS[preset_id]
                base_url = model_config.get("base_url") or preset_meta.get("base_url")
                api_key = model_config.get("api_key") or (os.getenv(preset_meta.get("api_key_env", "")) if preset_meta.get("api_key_env") else None)
                model = model_config.get("model") or preset_meta.get("model")
            else:
                base_url = model_config.get("base_url")
                api_key = model_config.get("api_key")
                model = model_config.get("model")
        else:
            base_url = model_config.get("base_url")
            api_key = model_config.get("api_key")
            model = model_config.get("model")

        client = _create_openai_client(base_url=base_url, api_key=api_key)

        context = LLMContext(
            session_id=session_id,
            user_query=str(message),
            system_prompt=system_prompt,
            messages=[{"role": "user", "content": str(message)}],
            tools=tools or [],
            extra_data={"attachments": attachments},
        )

        plugin_executor = PluginExecutor(plugins_cfg, context)
        await plugin_executor.run_pre_process()

        async def _execute_tool(func_name: str, args: dict[str, Any]) -> Any:
            tool_fn = context.tool_functions.get(func_name)
            if not tool_fn:
                raise ValueError(f"未注册的工具: {func_name}")
            result = tool_fn(**args)
            if inspect.iscoroutine(result):
                result = await result
            return result

        async def call_llm_with_context(ctx: LLMContext) -> dict[str, Any]:
            messages: list[dict[str, Any]] = []
            if ctx.system_prompt:
                messages.append({"role": "system", "content": ctx.system_prompt})
            messages.extend(ctx.messages or [])

            kwargs: dict[str, Any] = {"model": model or "", "temperature": temperature, "messages": messages}
            if ctx.tools:
                kwargs["tools"] = ctx.tools

            while True:
                resp = await client.chat.completions.create(**kwargs)
                choice = resp.choices[0]

                if choice.finish_reason == "tool_calls" and choice.message.tool_calls:
                    # 保存包含 tool_calls 的 assistant 消息
                    messages.append(choice.message.model_dump(exclude_none=True))

                    for tool_call in choice.message.tool_calls:
                        tool_name = tool_call.function.name
                        tool_args_str = tool_call.function.arguments or "{}"
                        try:
                            tool_args = json.loads(tool_args_str)
                        except Exception:
                            tool_args = {}

                        tool_result = await _execute_tool(tool_name, tool_args)
                        messages.append(
                            {
                                "role": "tool",
                                "tool_call_id": tool_call.id,
                                "content": json.dumps(tool_result),
                                "name": tool_name,
                            }
                        )

                    kwargs["messages"] = messages
                    # 继续下一轮，直到没有工具调用
                    continue

                break

            content = choice.message.content or ""
            return {"text": content, "raw": resp.model_dump()}

        result = await call_llm_with_context(context)

        context.ai_response_text = result.get("text", "")
        context.ai_raw_response = result.get("raw")

        await plugin_executor.run_post_process()

        text = context.ai_response_text or ""
        raw = context.ai_raw_response

        return {"text": text, "raw": raw}
