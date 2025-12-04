from enum import Enum


class LLMModel(Enum):
    LLAMA3 = 'llama3'
    LLAMA3_INSTRUCT = 'llama3:instruct'
    MISTRAL = 'mistral:latest'
    MISTRAL_INSTRUCT = 'mistral:instruct'
    LLAMA4 = 'llama4:latest'
    QWEN3 = 'qwen3:latest'
    GEMMA3 = 'gemma3:latest'
    DEEPSEEK = 'deepseek-r1:latest'
    GPT_4O_MINI = 'gpt-4o-mini'
    GPT_5_MINI = "gpt-5-mini"
