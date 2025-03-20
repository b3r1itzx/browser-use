import logging
import time
from functools import wraps
from typing import Any, Callable, Coroutine, Dict, List, Optional, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

# Storage for timing data
_TIMING_DATA: Dict[str, List[float]] = {}
_TIMING_CALLS: Dict[str, int] = {}
_TIMING_MAX: Dict[str, float] = {}
_TIMING_MIN: Dict[str, float] = {}

# Define generic type variables for return type and parameters
R = TypeVar('R')
P = ParamSpec('P')


def time_execution_sync(additional_text: str = '') -> Callable[[Callable[P, R]], Callable[P, R]]:
	def decorator(func: Callable[P, R]) -> Callable[P, R]:
		@wraps(func)
		def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
			start_time = time.time()
			result = func(*args, **kwargs)
			execution_time = time.time() - start_time
			
			# Store timing data with proper module prefix
			key = additional_text.strip('-').strip() if additional_text else func.__name__
			if '(' in key and ')' in key:
				# Extract module name from the format "--operation (module)"
				parts = key.split('(')
				if len(parts) > 1:
					module = parts[1].strip(')')
					operation = parts[0].strip('-').strip()
					key = f"{module}.{operation}"
			
			if key not in _TIMING_DATA:
				_TIMING_DATA[key] = []
				_TIMING_CALLS[key] = 0
				_TIMING_MAX[key] = 0.0
				_TIMING_MIN[key] = float('inf')
			
			_TIMING_DATA[key].append(execution_time)
			_TIMING_CALLS[key] += 1
			_TIMING_MAX[key] = max(_TIMING_MAX[key], execution_time)
			_TIMING_MIN[key] = min(_TIMING_MIN[key], execution_time)
			
			logger.debug(f'{additional_text} Execution time: {execution_time:.2f} seconds')
			return result

		return wrapper

	return decorator


def time_execution_async(
	additional_text: str = '',
) -> Callable[[Callable[P, Coroutine[Any, Any, R]]], Callable[P, Coroutine[Any, Any, R]]]:
	def decorator(func: Callable[P, Coroutine[Any, Any, R]]) -> Callable[P, Coroutine[Any, Any, R]]:
		@wraps(func)
		async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
			start_time = time.time()
			result = await func(*args, **kwargs)
			execution_time = time.time() - start_time
			
			# Store timing data with proper module prefix
			key = additional_text.strip('-').strip() if additional_text else func.__name__
			if '(' in key and ')' in key:
				# Extract module name from the format "--operation (module)"
				parts = key.split('(')
				if len(parts) > 1:
					module = parts[1].strip(')')
					operation = parts[0].strip('-').strip()
					key = f"{module}.{operation}"
			
			if key not in _TIMING_DATA:
				_TIMING_DATA[key] = []
				_TIMING_CALLS[key] = 0
				_TIMING_MAX[key] = 0.0
				_TIMING_MIN[key] = float('inf')
			
			_TIMING_DATA[key].append(execution_time)
			_TIMING_CALLS[key] += 1
			_TIMING_MAX[key] = max(_TIMING_MAX[key], execution_time)
			_TIMING_MIN[key] = min(_TIMING_MIN[key], execution_time)
			
			logger.debug(f'{additional_text} Execution time: {execution_time:.2f} seconds')
			return result

		return wrapper

	return decorator


def print_timing_summary(logger_name: Optional[str] = None):
    """
    Print a summary of execution times for all timed operations.
    
    Args:
        logger_name: If provided, only print operations related to this module
    """
    custom_logger = logging.getLogger(logger_name) if logger_name else logger
    custom_logger.info("===== TIMING SUMMARY =====")
    
    # Log all tracked operations to help with debugging
    if len(_TIMING_DATA) == 0:
        custom_logger.info("No timing data collected yet")
    else:
        custom_logger.info(f"All tracked operations: {list(_TIMING_DATA.keys())}")
    
    # Sort by total time spent (descending)
    sorted_operations = sorted(
        _TIMING_DATA.keys(),
        key=lambda op: sum(_TIMING_DATA[op]),
        reverse=True
    )
    
    filtered_operations = sorted_operations
    if logger_name:
        # Debug the filtering process
        custom_logger.info(f"Filtering for prefix '{logger_name}' from {len(sorted_operations)} operations")
        filtered_operations = [op for op in sorted_operations if logger_name.lower() in op.lower()]
    
    for op in filtered_operations:
        times = _TIMING_DATA[op]
        total_time = sum(times)
        calls = _TIMING_CALLS[op]
        avg_time = total_time / calls if calls > 0 else 0
        max_time = _TIMING_MAX[op]
        min_time = _TIMING_MIN[op]
        
        custom_logger.info(
            f"{op}: total={total_time:.4f}s calls={calls} avg={avg_time:.4f}s max={max_time:.4f}s min={min_time:.4f}s"
        )
    
    custom_logger.info("==========================")


def singleton(cls):
	instance = [None]

	def wrapper(*args, **kwargs):
		if instance[0] is None:
			instance[0] = cls(*args, **kwargs)
		return instance[0]

	return wrapper
