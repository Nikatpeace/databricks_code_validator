"""LLM Provider configuration management."""

import os
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class LLMProviderType(Enum):
    """Supported LLM provider types."""
    OPENAI = "openai"
    AZURE_OPENAI = "azure_openai"
    ANTHROPIC = "anthropic"
    DATABRICKS = "databricks"
    HUGGING_FACE = "hugging_face"
    CUSTOM = "custom"


@dataclass
class LLMProviderConfig:
    """Configuration for LLM providers."""
    provider_type: LLMProviderType
    api_key: str
    endpoint_url: Optional[str] = None
    model_name: Optional[str] = None
    max_tokens: Optional[int] = None
    temperature: Optional[float] = None
    timeout: Optional[int] = None


def get_llm_provider() -> LLMProviderConfig:
    """
    Get LLM provider configuration from environment variables.
    
    Returns:
        LLMProviderConfig: Configuration object for the LLM provider
        
    Raises:
        ValueError: If required environment variables are not set
    """
    provider_type_str = os.getenv("LLM_PROVIDER_TYPE")
    if not provider_type_str:
        raise ValueError("LLM_PROVIDER_TYPE environment variable is required")
    
    try:
        provider_type = LLMProviderType(provider_type_str.lower())
    except ValueError:
        valid_types = [t.value for t in LLMProviderType]
        raise ValueError(f"Invalid LLM_PROVIDER_TYPE: {provider_type_str}. Must be one of: {valid_types}")
    
    api_key = os.getenv("LLM_API_KEY")
    if not api_key:
        raise ValueError("LLM_API_KEY environment variable is required")
    
    endpoint_url = os.getenv("LLM_ENDPOINT_URL")
    model_name = os.getenv("LLM_MODEL_NAME")
    
    # Set defaults based on provider type
    if provider_type == LLMProviderType.OPENAI:
        endpoint_url = endpoint_url or "https://api.openai.com/v1/chat/completions"
        model_name = model_name or "gpt-3.5-turbo"
    elif provider_type == LLMProviderType.ANTHROPIC:
        endpoint_url = endpoint_url or "https://api.anthropic.com/v1/messages"
        model_name = model_name or "claude-3-sonnet-20240229"
    elif provider_type == LLMProviderType.DATABRICKS:
        if not endpoint_url:
            raise ValueError("LLM_ENDPOINT_URL is required for Databricks provider")
        model_name = model_name or "databricks-llama-2-70b-chat"
    elif provider_type == LLMProviderType.HUGGING_FACE:
        endpoint_url = endpoint_url or "https://api-inference.huggingface.co/models/"
        if not model_name:
            raise ValueError("LLM_MODEL_NAME is required for Hugging Face provider")
    
    return LLMProviderConfig(
        provider_type=provider_type,
        api_key=api_key,
        endpoint_url=endpoint_url,
        model_name=model_name,
        max_tokens=int(os.getenv("LLM_MAX_TOKENS", "2000")),
        temperature=float(os.getenv("LLM_TEMPERATURE", "0.1")),
        timeout=int(os.getenv("LLM_TIMEOUT", "30"))
    )


def get_provider_examples() -> Dict[str, Dict[str, Any]]:
    """
    Get example configurations for different LLM providers.
    
    Returns:
        Dict containing example configurations for each provider
    """
    return {
        "openai": {
            "api_key": "sk-your-openai-api-key",
            "endpoint_url": "https://api.openai.com/v1/chat/completions",
            "model_name": "gpt-3.5-turbo"
        },
        "azure_openai": {
            "api_key": "your-azure-openai-key",
            "endpoint_url": "https://your-resource.openai.azure.com/openai/deployments/your-deployment/chat/completions?api-version=2023-12-01-preview",
            "model_name": "gpt-35-turbo"
        },
        "anthropic": {
            "api_key": "your-anthropic-api-key",
            "endpoint_url": "https://api.anthropic.com/v1/messages",
            "model_name": "claude-3-sonnet-20240229"
        },
        "databricks": {
            "api_key": "dapi-your-databricks-token",
            "endpoint_url": "https://your-workspace.cloud.databricks.com/serving-endpoints/your-endpoint/invocations",
            "model_name": "databricks-llama-2-70b-chat"
        },
        "hugging_face": {
            "api_key": "hf_your-hugging-face-token",
            "endpoint_url": "https://api-inference.huggingface.co/models/",
            "model_name": "microsoft/DialoGPT-medium"
        }
    }


def create_openai_config(api_key: str, model_name: str = "gpt-3.5-turbo") -> LLMProviderConfig:
    """Create OpenAI provider configuration."""
    return LLMProviderConfig(
        provider_type=LLMProviderType.OPENAI,
        api_key=api_key,
        endpoint_url="https://api.openai.com/v1/chat/completions",
        model_name=model_name
    )


def create_anthropic_config(api_key: str, model_name: str = "claude-3-sonnet-20240229") -> LLMProviderConfig:
    """Create Anthropic provider configuration."""
    return LLMProviderConfig(
        provider_type=LLMProviderType.ANTHROPIC,
        api_key=api_key,
        endpoint_url="https://api.anthropic.com/v1/messages",
        model_name=model_name
    )


def create_databricks_config(api_key: str, endpoint_url: str, model_name: str) -> LLMProviderConfig:
    """Create Databricks provider configuration."""
    return LLMProviderConfig(
        provider_type=LLMProviderType.DATABRICKS,
        api_key=api_key,
        endpoint_url=endpoint_url,
        model_name=model_name
    )