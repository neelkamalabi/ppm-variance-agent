from src.database import Database
from typing import Any, Dict


def execute_sql_function(sql_query: str) -> Dict[str, Any]:
    """
    Execute a SQL SELECT query against the Zero-Based Budgeting MySQL database and return the results. Use this function to answer user questions about budget data.

    Args:
        sql_query (str): The SQL SELECT query to execute. Must be a valid SELECT statement for MySQL. Can include CTEs (WITH clauses). Never include INSERT, UPDATE, DELETE, or DROP statements.

    Returns:
        Dict[str, Any]: Dictionary with query results or error message
    """
    try:
        # Print the SQL query being executed
        print(f"\n📝 Executing SQL Query:\n{sql_query}\n")

        # Security check - only allow SELECT queries
        sql_upper = sql_query.strip().upper()
        if not sql_upper.startswith("SELECT"):
            return {
                "error": "Only SELECT queries are allowed. Security check failed.",
                "sql_query": sql_query,
            }

        # Initialize database connection
        db = Database()

        # Execute the query
        results = db.execute_query(sql_query)
        print(f"✅ Query executed successfully, returned {len(results)} rows.\n")
        print(results)

        return {
            "success": True,
            "results": results,
            "row_count": len(results),
            "sql_query": sql_query,
        }
    except Exception as e:
        return {"success": False, "error": str(e), "sql_query": sql_query}
