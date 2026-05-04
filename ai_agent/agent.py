import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import duckdb
from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.prebuilt import create_react_agent

from ai_agent.llm import llm
from ai_agent.prompts import ANALYST_PROMPT, PLANNER_PROMPT
from ai_agent.schemas import AgentState, PlannerOutput, SQLToolInput

_DB_PATH = _ROOT / "database" / "spark_telemetry.duckdb"


@tool(args_schema=SQLToolInput)
def execute_sql(query: str) -> str:
    """
    Executes a SQL query against the DuckDB Spark Telemetry database.
    Always write standard SQL. The database contains historical Spark metrics.
    """
    print(f"\n[🔧 Tool Execution] Running SQL:\n{query}\n")
    try:
        con = duckdb.connect(database=str(_DB_PATH), read_only=True)
        df = con.execute(query).df()
        con.close()

        if df.empty:
            return "Query executed successfully but returned 0 rows."

        res = df.to_markdown(index=False)
        return res[:4000] + "\n...[TRUNCATED]" if len(res) > 4000 else res
    except Exception as e:
        return f"SQL Execution Error: {str(e)}\nFix your query and try again."


sql_analyst_agent = create_react_agent(llm, tools=[execute_sql], prompt=ANALYST_PROMPT)
structured_planner_llm = llm.with_structured_output(PlannerOutput)


def planner_node(state: AgentState):
    app_id = state["app_id"]
    messages = state["messages"]

    sys_msg = f"{PLANNER_PROMPT}\n\nCurrently investigating app_id: {app_id}"

    structured_response = structured_planner_llm.invoke(
        [{"role": "system", "content": sys_msg}] + list(messages)
    )

    if structured_response.is_complete:
        final_text = (
            f"**FINAL REPORT**\n\n"
            f"**Root Cause (Plain English):**\n{structured_response.root_cause_simple}\n\n"
            f"**Spark UI Mapping:**\n{structured_response.spark_ui_mapping}\n\n"
            f"**Recommended Fix:**\n{structured_response.recommended_fix}"
        )
        return {"messages": [AIMessage(content=final_text)], "is_complete": True}
    return {
        "messages": [AIMessage(content=structured_response.message_to_analyst)],
        "is_complete": False,
    }


def analyst_node(state: AgentState):
    messages = state["messages"]
    analyst_response = sql_analyst_agent.invoke({"messages": messages})
    final_output = analyst_response["messages"][-1]

    return {"messages": [AIMessage(content=f"[SQL Analyst]: {final_output.content}")]}


def routing_logic(state: AgentState):
    if state["is_complete"]:
        return END
    return "sql_analyst"


workflow = StateGraph(AgentState)
workflow.add_node("planner", planner_node)
workflow.add_node("sql_analyst", analyst_node)

workflow.set_entry_point("planner")

workflow.add_conditional_edges(
    "planner",
    routing_logic,
    {
        "sql_analyst": "sql_analyst",
        END: END,
    },
)
workflow.add_edge("sql_analyst", "planner")

optimizer_app = workflow.compile()


if __name__ == "__main__":
    from IPython.display import Image, display

    try:
        img_data = optimizer_app.get_graph().draw_mermaid_png()
        display(Image(img_data))
    except Exception as e:
        print(f"Could not generate PNG: {e}")

    TARGET_APP_ID = "app-20260502070220-0013"

    print(f"🤖 Starting Structured Investigation for: {TARGET_APP_ID}\n")

    inputs = {
        "messages": [HumanMessage(content=f"Please analyze app {TARGET_APP_ID}.")],
        "app_id": TARGET_APP_ID,
        "is_complete": False,
    }

    for output in optimizer_app.stream(inputs, {"recursion_limit": 20}):
        for key, value in output.items():
            print(f"\n### {key.upper()} ###")
            print(value["messages"][-1].content)
            print("-" * 50)
