"""Spark utilities for the code standards bot."""

from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import SparkSession
else:
    try:
        from pyspark.sql import SparkSession
    except ImportError:
        SparkSession = None


def create_spark_session(
    app_name: str = "CodeStandardsBot",
    config: Optional[dict] = None
) -> Optional[SparkSession]:
    """
    Create a Spark session.
    
    Args:
        app_name: Name of the Spark application
        config: Optional Spark configuration dictionary
        
    Returns:
        SparkSession or None if PySpark not available
    """
    if SparkSession is None:
        raise ImportError("PySpark is not available. Please install it to use Spark functionality.")
    
    builder = SparkSession.builder.appName(app_name)
    
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
    
    return builder.getOrCreate()


def get_or_create_spark_session(
    app_name: str = "CodeStandardsBot"
) -> Optional[SparkSession]:
    """
    Get existing Spark session or create a new one.
    
    Args:
        app_name: Name of the Spark application
        
    Returns:
        SparkSession or None if PySpark not available
    """
    if SparkSession is None:
        return None
    
    try:
        # Try to get existing session
        return SparkSession.getActiveSession()
    except Exception:
        # Create new session if none exists
        return create_spark_session(app_name)


def save_dataframe_to_table(
    df, 
    table_name: str, 
    mode: str = "overwrite",
    format: str = "delta"
) -> None:
    """
    Save a DataFrame to a table.
    
    Args:
        df: Spark DataFrame
        table_name: Name of the table to save to
        mode: Write mode (overwrite, append, etc.)
        format: Table format (delta, parquet, etc.)
    """
    if df is None:
        raise ValueError("DataFrame cannot be None")
    
    df.write.format(format).mode(mode).saveAsTable(table_name) 