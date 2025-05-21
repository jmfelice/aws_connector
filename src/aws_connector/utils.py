"""Common utilities for AWS connector modules."""

import logging

def setup_logging(name: str) -> logging.Logger:
    """Set up logging.
    
    Args:
        name (str): The name for the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    # Configure root logger first
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove any existing handlers from root logger
    root_logger.handlers = []
    
    # Create a simple formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create a handler and set the formatter
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    
    # Add handler to root logger
    root_logger.addHandler(handler)
    
    # Get the specific logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    return logger