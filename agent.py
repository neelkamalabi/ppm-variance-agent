import os
from dotenv import load_dotenv
from typing import Any, Callable, Set
from azure.identity import DefaultAzureCredential
from azure.ai.agents import AgentsClient
from azure.ai.agents.models import FunctionTool, ToolSet, MessageRole, ListSortOrder

from src.database import Database
from src.sql_tool import execute_sql_function


def system_message():
    """Initialize the system message with schema information."""
    db = Database()
    schema_info = db.get_schema_info()

    system_content = f"""You are a SQL expert agent specializing in Zero-Based Budgeting databases. 
Your task is to help users query budget data by converting their natural language questions into SQL queries.

{schema_info}

Guidelines:
1. Always use proper SQL syntax for MySQL
2. Use JOINs to connect related tables
3. Use appropriate aggregate functions (SUM, COUNT, AVG, MAX, MIN) when needed
4. Format dates using MySQL date functions (DATE_FORMAT, etc.)
5. Use table aliases for readability (d for departments, bp for budget_periods, etc.)
6. Always use proper WHERE clauses for filtering
7. For amounts, use DECIMAL precision (they are stored as DECIMAL(18,2))
8. Only generate SELECT queries - never INSERT, UPDATE, DELETE, or DROP statements
9. If the question asks for totals or sums, use SUM() function
10. Use proper column names from the schema
11. Use LIMIT instead of TOP for limiting results
12. Use MySQL-specific syntax and functions

When a user asks a question, use the execute_sql_query function to execute the SQL query and return the results.
"""

    return system_content


def main():

    # Clear console
    os.system("cls" if os.name == "nt" else "clear")

    # Load env vars
    load_dotenv()
    project_endpoint = os.getenv("PROJECT_ENDPOINT")
    model_deployment = os.getenv("MODEL_DEPLOYMENT_NAME")

    if not project_endpoint or not model_deployment:
        raise ValueError("Missing PROJECT_ENDPOINT or MODEL_DEPLOYMENT_NAME")

    # Connect to Azure AI Studio Agent runtime
    agent_client = AgentsClient(
        endpoint=project_endpoint,
        credential=DefaultAzureCredential(
            exclude_environment_credential=True,
            exclude_managed_identity_credential=True,
        ),
    )

    # Define a set of callable functions
    user_functions: Set[Callable[..., Any]] = {execute_sql_function}

    with agent_client:

        # Register Python tools
        functions = FunctionTool(user_functions)
        toolset = ToolSet()
        toolset.add(functions)

        agent_client.enable_auto_function_calls(toolset)

        # Create the Finance Agent
        agent = agent_client.create_agent(
            model=model_deployment,
            name="finance-variance-agent",
            instructions=system_message(),
            toolset=toolset,
        )

        # Create a conversation thread
        thread = agent_client.threads.create()
        print(f"Agent ready: {agent.name} ({agent.id})")

        # -----------------------------
        # Interactive chat loop
        # -----------------------------
        while True:
            user_prompt = input(
                "\nAsk a variance question (or type 'quit', 'exit', or 'q'): "
            )
            if user_prompt.lower() in ["quit", "exit", "q"]:
                break
            if not user_prompt.strip():
                continue

            agent_client.messages.create(
                thread_id=thread.id,
                role=MessageRole.USER,
                content=user_prompt,
            )

            run = agent_client.runs.create_and_process(
                thread_id=thread.id,
                agent_id=agent.id,
            )

            if run.status == "failed":
                print(f"Run failed: {run.last_error}")
                continue

            response = agent_client.messages.get_last_message_text_by_role(
                thread_id=thread.id,
                role=MessageRole.AGENT,
            )

            if response:
                print("\n--- Agent Response ---")
                print(response.text.value)

        # -----------------------------
        # Conversation log (optional)
        # -----------------------------
        print("\nConversation History:\n")
        messages = agent_client.messages.list(
            thread_id=thread.id, order=ListSortOrder.ASCENDING
        )

        for msg in messages:
            if msg.text_messages:
                print(f"{msg.role}: {msg.text_messages[-1].text.value}\n")

        # Cleanup (optional)
        agent_client.delete_agent(agent.id)
        print("Agent deleted")


if __name__ == "__main__":
    main()
