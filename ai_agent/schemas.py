from typing import Annotated, Optional, Sequence, TypedDict

from langchain_core.messages import BaseMessage
from langgraph.graph import add_messages
from pydantic import BaseModel, Field


class SQLToolInput(BaseModel):
    """Input schema for the execute_sql tool."""

    query: str = Field(description="A valid, raw DuckDB SQL query string. Do not include markdown formatting.")


class PlannerOutput(BaseModel):
    """Structured response schema for the Lead Spark Architect."""

    is_complete: bool = Field(description="Set to True ONLY if you have found the root cause. Otherwise, False.")
    message_to_analyst: Optional[str] = Field(description="If is_complete is False, write your next instruction for the SQL Analyst.")
    root_cause_simple: Optional[str] = Field(description="If is_complete is True, explain the root cause in plain, non-jargony English that a mid-level Data Engineer can easily understand.")
    spark_ui_mapping: Optional[str] = Field(description="If is_complete is True, map the evidence exactly to the Spark UI. Use headers like 'SQL Tab:', 'Jobs/Stages Tab:', and 'Executors Tab:' so the user can verify your claims.")
    recommended_fix: Optional[str] = Field(description="If is_complete is True, provide the exact PySpark code or config changes needed.")


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]
    app_id: str
    is_complete: bool
