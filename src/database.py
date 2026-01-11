"""Database connection and query execution utilities."""

import os
import pymysql
import pymysql.cursors
from pymysql import Error
from typing import List, Dict, Any, Optional

# load environment variables from .env file
from dotenv import load_dotenv

load_dotenv()


class Database:
    """Database connection and query execution class."""

    def __init__(self):
        """Initialize database connection."""
        self.connection_params = self._get_config()
        self.conn: Optional[pymysql.Connection] = None

    def _get_config(self):
        """Get database configuration from environment variables."""
        host = os.getenv("MYSQL_HOST", "localhost")
        port = int(os.getenv("MYSQL_PORT", "3306"))
        database = os.getenv("MYSQL_DATABASE", "budget_db")
        username = os.getenv("MYSQL_USERNAME", "root")
        password = os.getenv("MYSQL_PASSWORD", "")
        ssl_disabled = os.getenv("MYSQL_SSL_DISABLED", "False").lower() == "true"
        return {
            "host": host,
            "port": port,
            "database": database,
            "user": username,
            "password": password,
            "autocommit": False,
        }

    def connect(self):
        """Establish connection to MySQL Database."""
        try:
            self.conn = pymysql.connect(**self.connection_params)
            return True
        except Error as e:
            raise ConnectionError(f"Failed to connect to database: {str(e)}")

    def disconnect(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
            self.conn = None

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Execute a SELECT query and return results as a list of dictionaries.

        Args:
            query: SQL query string
            params: Optional dictionary of parameters for parameterized queries

        Returns:
            List of dictionaries representing rows
        """
        if not self.conn:
            self.connect()

        try:
            # PyMySQL uses DictCursor for dictionary results
            cursor = self.conn.cursor(pymysql.cursors.DictCursor)

            # Execute query with or without parameters
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            # Fetch all rows (already dictionaries with DictCursor)
            results = cursor.fetchall()

            cursor.close()
            return results

        except Error as e:
            raise RuntimeError(f"Query execution failed: {str(e)}")

    def execute_non_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Execute a non-SELECT query (INSERT, UPDATE, DELETE, etc.).

        Args:
            query: SQL query string
            params: Optional dictionary of parameters for parameterized queries

        Returns:
            Number of rows affected
        """
        if not self.conn:
            self.connect()

        try:
            cursor = self.conn.cursor()

            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)

            rows_affected = cursor.rowcount
            self.conn.commit()
            cursor.close()

            return rows_affected

        except Error as e:
            if self.conn:
                self.conn.rollback()
            raise RuntimeError(f"Query execution failed: {str(e)}")

    def execute_script(self, script: str):
        """
        Execute a SQL script (multiple statements).

        Args:
            script: SQL script string containing multiple statements
        """
        if not self.conn:
            self.connect()

        try:
            cursor = self.conn.cursor()

            # Split script by semicolons and execute each statement
            statements = [s.strip() for s in script.split(";") if s.strip()]

            for statement in statements:
                if statement:
                    cursor.execute(statement)

            self.conn.commit()
            cursor.close()

        except Error as e:
            if self.conn:
                self.conn.rollback()
            raise RuntimeError(f"Script execution failed: {str(e)}")

    def get_schema_info(self) -> str:
        """
        Get database schema information for NL to SQL conversion.

        Returns:
            String containing schema information
        """
        schema_info = """
Database Schema for Zero-Based Budgeting (MySQL):

Table: departments
Columns:
- id (INT, PRIMARY KEY): Unique identifier for department
- name (VARCHAR): Department name
- code (VARCHAR): Department code (unique)
- created_at (TIMESTAMP): Creation timestamp (auto-set on insert)

Table: budget_periods
Columns:
- id (INT, PRIMARY KEY): Unique identifier for budget period
- period_name (VARCHAR): Name of the period (e.g., "Q1 2024")
- start_date (DATE): Period start date
- end_date (DATE): Period end date
- status (VARCHAR): Status (Draft, Approved, Closed)
- created_at (TIMESTAMP): Creation timestamp (auto-set on insert)

Table: budget_categories
Columns:
- id (INT, PRIMARY KEY): Unique identifier for category
- category_name (VARCHAR): Category name
- description (VARCHAR): Category description
- created_at (TIMESTAMP): Creation timestamp (auto-set on insert)

Table: budget_items
Columns:
- id (INT, PRIMARY KEY): Unique identifier for budget item
- department_id (INT, FOREIGN KEY -> departments.id): Department reference
- period_id (INT, FOREIGN KEY -> budget_periods.id): Budget period reference
- category_id (INT, FOREIGN KEY -> budget_categories.id): Category reference
- budgeted_amount (DECIMAL): Budgeted amount
- justification (VARCHAR): Justification for the budget item
- status (VARCHAR): Status (Draft, Submitted, Approved, Rejected)
- created_at (TIMESTAMP): Creation timestamp (auto-set on insert)

Table: actual_expenses
Columns:
- id (INT, PRIMARY KEY): Unique identifier for expense
- budget_item_id (INT, FOREIGN KEY -> budget_items.id): Budget item reference
- amount (DECIMAL): Expense amount
- expense_date (DATE): Date of expense
- description (VARCHAR): Expense description
- created_at (TIMESTAMP): Creation timestamp (auto-set on insert)

Common Query Patterns:
- To get budget totals by department: JOIN budget_items with departments
- To get budget by period: JOIN budget_items with budget_periods
- To get actual vs budgeted: JOIN budget_items with actual_expenses
- To filter by status: Use WHERE clause on status columns
- Amounts are stored as DECIMAL(18,2) - use SUM() for aggregations
- Use LIMIT instead of TOP for limiting results
- Use MySQL date functions (DATE_FORMAT, etc.) instead of SQL Server functions
"""
        return schema_info

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


if __name__ == "__main__":
    # Simple test of database connection and query execution
    db = Database()
    try:
        db.connect()
        print("Connected to database.")

        # Test a simple query
        results = db.execute_query("SELECT * from actual_expenses LIMIT 1;")
        print("Query Results:", results)

    except Exception as e:
        print("Error:", str(e))
    finally:
        db.disconnect()
        print("Disconnected from database.")
