import gc
import json
import logging
import asyncio
from dataclasses import dataclass
from importlib import resources
from typing import TYPE_CHECKING, Optional, Dict, Any
import time
from functools import wraps

if TYPE_CHECKING:
	from playwright.async_api import Page

from browser_use.dom.views import (
	DOMBaseNode,
	DOMElementNode,
	DOMState,
	DOMTextNode,
	SelectorMap,
)
from browser_use.utils import time_execution_async

logger = logging.getLogger(__name__)

# Global timer dictionary to store all timing measurements
TIMERS: Dict[str, Dict[str, Any]] = {
    "total_time": {},
    "call_count": {},
    "max_time": {},
    "min_time": {}
}

def timer(name):
    """Decorator to time function execution and log results."""
    def decorator(func):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            result = await func(*args, **kwargs)
            end = time.time()
            duration = end - start
            
            # Store timing data
            if name not in TIMERS["total_time"]:
                TIMERS["total_time"][name] = 0
                TIMERS["call_count"][name] = 0
                TIMERS["max_time"][name] = 0
                TIMERS["min_time"][name] = float('inf')
                
            TIMERS["total_time"][name] += duration
            TIMERS["call_count"][name] += 1
            TIMERS["max_time"][name] = max(TIMERS["max_time"][name], duration)
            TIMERS["min_time"][name] = min(TIMERS["min_time"][name], duration)
            
            logger.info(f"TIMER: {name} - {duration:.4f}s (avg: {TIMERS['total_time'][name]/TIMERS['call_count'][name]:.4f}s)")
            return result
            
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            duration = end - start
            
            # Store timing data
            if name not in TIMERS["total_time"]:
                TIMERS["total_time"][name] = 0
                TIMERS["call_count"][name] = 0
                TIMERS["max_time"][name] = 0
                TIMERS["min_time"][name] = float('inf')
                
            TIMERS["total_time"][name] += duration
            TIMERS["call_count"][name] += 1
            TIMERS["max_time"][name] = max(TIMERS["max_time"][name], duration)
            TIMERS["min_time"][name] = min(TIMERS["min_time"][name], duration)
            
            logger.info(f"TIMER: {name} - {duration:.4f}s (avg: {TIMERS['total_time'][name]/TIMERS['call_count'][name]:.4f}s)")
            return result
            
        # Return appropriate wrapper based on function type
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper
    return decorator

def print_timing_summary():
    """Print a summary of all timings collected."""
    logger.info("===== TIMING SUMMARY =====")
    
    # Sort by total time descending
    sorted_timers = sorted(TIMERS["total_time"].items(), key=lambda x: x[1], reverse=True)
    
    # Copy timing data to the utils timing system
    from browser_use.utils import _TIMING_DATA, _TIMING_CALLS, _TIMING_MAX, _TIMING_MIN
    for name, total in sorted_timers:
        dom_key = f"dom.{name}"
        calls = TIMERS["call_count"][name]
        avg = total / calls
        max_time = TIMERS["max_time"][name]
        min_time = TIMERS["min_time"][name]
        
        # Store in the utils system
        if dom_key not in _TIMING_DATA:
            _TIMING_DATA[dom_key] = []
            _TIMING_CALLS[dom_key] = calls
            _TIMING_MAX[dom_key] = max_time
            _TIMING_MIN[dom_key] = min_time
        else:
            _TIMING_CALLS[dom_key] = calls
            _TIMING_MAX[dom_key] = max(_TIMING_MAX[dom_key], max_time)
            _TIMING_MIN[dom_key] = min(_TIMING_MIN[dom_key], min_time)
        
        # Add the total time as a single entry
        _TIMING_DATA[dom_key] = [total]
        
        logger.info(f"{name}: total={total:.4f}s calls={calls} avg={avg:.4f}s max={max_time:.4f}s min={min_time:.4f}s")
    
    logger.info("==========================")

def get_timing_summary_dict():
    """Get timing summary as a dictionary."""
    summary = {}
    
    for name, total in TIMERS["total_time"].items():
        calls = TIMERS["call_count"][name]
        avg = total / calls
        max_time = TIMERS["max_time"][name]
        min_time = TIMERS["min_time"][name]
        
        summary[name] = {
            "total": total,
            "calls": calls,
            "avg": avg,
            "max": max_time,
            "min": min_time
        }
    
    return summary

@dataclass
class ViewportInfo:
	width: int
	height: int


class DomService:
	# Load JS code once at class level instead of per-instance
	JS_CODE = resources.read_text('browser_use.dom', 'buildDomTree.js')

	def __init__(self, page: 'Page'):
		self.page = page
		self.xpath_cache = {}
		self.js_code = self.JS_CODE  # Reference the class variable

	# region - Clickable elements
	@time_execution_async('--get_clickable_elements')
	@timer("get_clickable_elements")
	async def get_clickable_elements(
		self,
		highlight_elements: bool = True,
		focus_element: int = -1,
		viewport_expansion: int = 0,
	) -> DOMState:
		"""Get clickable elements from the page DOM."""
		# Generate a cache key based on the parameters
		cache_key = f"clickable_{highlight_elements}_{focus_element}_{viewport_expansion}"
		
		# Check if we have a recent cached result
		if hasattr(self, '_clickable_elements_cache') and self._clickable_elements_cache.get('key') == cache_key:
			# Only use cache if it's recent (less than 300ms old)
			if time.time() - self._clickable_elements_cache.get('time', 0) < 0.3:
				logger.info("TIMER: Using cached clickable elements (cached < 300ms ago)")
				return self._clickable_elements_cache['result']
		
		# Get the DOM tree
		element_tree, selector_map = await self._build_dom_tree(highlight_elements, focus_element, viewport_expansion)
		state = DOMState(element_tree=element_tree, selector_map=selector_map)
		
		# Cache the result
		self._clickable_elements_cache = {
			'key': cache_key,
			'time': time.time(),
			'result': state
		}
		
		return state

	@time_execution_async('--build_dom_tree')
	@timer("build_dom_tree")
	async def _build_dom_tree(
		self,
		highlight_elements: bool,
		focus_element: int,
		viewport_expansion: int,
	) -> tuple[DOMElementNode, SelectorMap]:
		# Check if we can use cached result when parameters haven't changed
		cache_key = f"{highlight_elements}_{focus_element}_{viewport_expansion}"
		if hasattr(self, '_dom_tree_cache') and self._dom_tree_cache.get('key') == cache_key:
			# Only use cache if it's recent (less than 500ms old)
			if time.time() - self._dom_tree_cache.get('time', 0) < 0.5:
				logger.info("TIMER: Using cached DOM tree (cached < 500ms ago)")
				return self._dom_tree_cache['result']
		
		args = {
			'doHighlightElements': highlight_elements,
			'focusHighlightIndex': focus_element,
			'viewportExpansion': viewport_expansion,
			'debugMode': True,  # Always enable debug mode for performance tracking
			'useCompression': True,  # Enable compression for large DOMs
		}
		
		t0 = time.time()
		# Use try/except for better error handling
		try:
			result = await self.page.evaluate(self.js_code, args)
		except Exception as e:
			logger.error(f"Error evaluating JavaScript: {str(e)}")
			raise
		
		t_evaluate = time.time()
		logger.info(f"TIMER: page.evaluate - {t_evaluate-t0:.4f}s")
		
		# Handle the JSON string returned from JavaScript
		try:
			if isinstance(result, str):
				t_before_parse = time.time()
				eval_page = json.loads(result)
				t_after_parse = time.time()
				json_parse_time = t_after_parse-t_before_parse
				logger.info(f"TIMER: json.loads - {json_parse_time:.4f}s (size: {len(result)} bytes)")
				
				# Use orjson for faster parsing if it's taking too long
				if json_parse_time > 0.1 and len(result) > 100000:
					try:
						import orjson
						t_before_orjson = time.time()
						eval_page = orjson.loads(result)
						t_after_orjson = time.time()
						logger.info(f"TIMER: orjson.loads - {t_after_orjson-t_before_orjson:.4f}s - {(json_parse_time/(t_after_orjson-t_before_orjson)):.2f}x faster")
					except ImportError:
						logger.info("TIMER: orjson not available. Consider installing for faster JSON parsing")
			else:
				# Backward compatibility with older versions
				eval_page = result
				logger.info(f"TIMER: using direct result (not JSON string)")
		except json.JSONDecodeError:
			# If JSON parsing fails, assume it's not a string that needs parsing
			eval_page = result
			logger.info(f"TIMER: JSON decode error, using direct result")
		
		t9 = time.time()
		logger.info(f"buildDomTree.js time: {t9-t0:.3f}(Sec)")
		
		# Print JS performance summary if available
		if isinstance(eval_page, dict) and 'perfSummary' in eval_page:
			js_perf = eval_page['perfSummary']
			logger.info(f"===== JS PERFORMANCE SUMMARY =====")
			logger.info(f"Total JS processing time: {js_perf['totalTimeMs']}ms for {js_perf['nodeCount']} nodes")
			
			# Log top 5 sections by percentage
			sections = sorted(
				[(k, v) for k, v in js_perf['sections'].items()], 
				key=lambda x: float(x[1]['percentage']), 
				reverse=True
			)[:5]  # Just top 5 to keep logs reasonable
			
			for section, data in sections:
				logger.info(f"  {section}: {data['timeMs']}ms ({data['percentage']}%) - {data['calls']} calls")
			
			# Log top 5 operations by percentage
			if 'operations' in js_perf:
				operations = sorted(
					[(k, v) for k, v in js_perf['operations'].items()], 
					key=lambda x: float(x[1]['percentage']), 
					reverse=True
				)[:5]  # Just top 5 to keep logs reasonable
				
				for op, data in operations:
					logger.info(f"  {op}: {data['timeMs']}ms ({data['percentage']}%) - {data['calls']} calls, avg {data['avgTimeMs']}ms per call")
			
			logger.info("==================================")
		
		# Use construct_dom_tree for complex JSON structure with node map
		if isinstance(eval_page, dict) and 'map' in eval_page and 'rootId' in eval_page:
			t_construct_start = time.time()
			result = await self._construct_dom_tree(eval_page)
			t_construct_end = time.time()
			logger.info(f"TIMER: _construct_dom_tree - {t_construct_end-t_construct_start:.4f}s")
			
			# Cache the result for future use
			self._dom_tree_cache = {
				'key': cache_key,
				'time': time.time(),
				'result': result
			}
			
			return result
		
		# Fallback to simple node parsing for direct node data
		if not isinstance(eval_page, dict):
			raise ValueError(f"Expected dict, got {type(eval_page)}: {eval_page}")
		
		t_parse_start = time.time()
		html_to_dict = self._parse_node(eval_page)
		t_parse_end = time.time()
		logger.info(f"TIMER: _parse_node - {t_parse_end-t_parse_start:.4f}s")
		
		if html_to_dict is None or not isinstance(html_to_dict, DOMElementNode):
			raise ValueError('Failed to parse HTML to dictionary')
		
		# Create selector map by traversing the tree
		selector_map = {}
		
		t_collect_start = time.time()
		def _collect_interactive_elements(node):
			if isinstance(node, DOMElementNode) and node.highlight_index is not None:
				selector_map[node.highlight_index] = node
				
			if isinstance(node, DOMElementNode):
				for child in node.children:
					_collect_interactive_elements(child)
		
		_collect_interactive_elements(html_to_dict)
		t_collect_end = time.time()
		logger.info(f"TIMER: _collect_interactive_elements - {t_collect_end-t_collect_start:.4f}s")
		
		# Cache the result for future use
		self._dom_tree_cache = {
			'key': cache_key,
			'time': time.time(),
			'result': (html_to_dict, selector_map)
		}
		
		# Print Python performance summary at the end of each run
		print_timing_summary()
		
		return html_to_dict, selector_map

	@time_execution_async('--construct_dom_tree')
	@timer("construct_dom_tree")
	async def _construct_dom_tree(
		self,
		eval_page: dict,
	) -> tuple[DOMElementNode, SelectorMap]:
		js_node_map = eval_page['map']
		js_root_id = eval_page['rootId']
		node_count = len(js_node_map)
		
		logger.info(f"TIMER: DOM size - {node_count} nodes")

		# Preallocate dictionaries with expected size
		selector_map = {}
		node_map = {}
		
		# Identify highlighted nodes first to avoid unnecessary processing
		highlight_indices = {}
		if node_count > 10000:  # Only do this optimization for large DOMs
			for id, node_data in js_node_map.items():
				if 'highlightIndex' in node_data and node_data['highlightIndex'] is not None:
					highlight_indices[id] = node_data['highlightIndex']
		
		# Use batch processing for very large DOMs
		BATCH_SIZE = 5000
		use_batching = node_count > BATCH_SIZE
		
		# First pass: create all nodes
		t_first_pass_start = time.time()
		nodes_processed = 0
		
		# For large DOMs, process in batches to avoid memory pressure
		if use_batching:
			logger.info(f"TIMER: Using batch processing for large DOM ({node_count} nodes)")
			batch_keys = list(js_node_map.keys())
			current_batch = 0
			
			while current_batch * BATCH_SIZE < node_count:
				start_idx = current_batch * BATCH_SIZE
				end_idx = min((current_batch + 1) * BATCH_SIZE, node_count)
				batch = batch_keys[start_idx:end_idx]
				
				for id in batch:
					node_data = js_node_map[id]
					# Skip non-highlighted nodes for large DOMs if we already identified them
					if len(highlight_indices) > 0 and id not in highlight_indices and 'children' not in node_data:
						# Skip leaf nodes that aren't highlighted to save memory
						continue
					
					node = self._parse_node(node_data)
					nodes_processed += 1
					
					if node is None:
						continue

					node_map[id] = node

					if isinstance(node, DOMElementNode) and node.highlight_index is not None:
						selector_map[node.highlight_index] = node
				
				logger.info(f"TIMER: First pass batch {current_batch+1} - processed {nodes_processed}/{node_count} nodes")
				current_batch += 1
				
				# Force garbage collection between batches
				if current_batch % 5 == 0:
					gc.collect()
		else:
			# Process directly for smaller DOMs
			for id, node_data in js_node_map.items():
				node = self._parse_node(node_data)
				nodes_processed += 1
				
				if nodes_processed % 1000 == 0:
					logger.info(f"TIMER: First pass progress - {nodes_processed}/{node_count} nodes processed")
				
				if node is None:
					continue

				node_map[id] = node

				if isinstance(node, DOMElementNode) and node.highlight_index is not None:
					selector_map[node.highlight_index] = node
				
		t_first_pass_end = time.time()
		logger.info(f"TIMER: First pass - {t_first_pass_end-t_first_pass_start:.4f}s for {nodes_processed} nodes")

		# Second pass: build the tree structure
		t_second_pass_start = time.time()
		nodes_connected = 0
		children_connected = 0
		
		# Create a lookup for parent-child relationships to avoid multiple iterations
		if use_batching:
			parent_child_map = {}
			for id, node_data in js_node_map.items():
				if id not in node_map:
					continue
					
				if not isinstance(node_map[id], DOMElementNode) or 'children' not in node_data:
					continue
					
				parent_child_map[id] = node_data['children']
				
			# Process parent-child relationships
			for parent_id, child_ids in parent_child_map.items():
				parent_node = node_map[parent_id]
				nodes_connected += 1
				
				valid_children = []
				for child_id in child_ids:
					if child_id in node_map:
						valid_children.append(child_id)
				
				# Preallocation of children list for better performance
				if len(valid_children) > 0:
					parent_node.children = [None] * len(valid_children)
					
					for i, child_id in enumerate(valid_children):
						child_node = node_map[child_id]
						child_node.parent = parent_node
						parent_node.children[i] = child_node
						children_connected += 1
				
				if nodes_connected % 1000 == 0:
					logger.info(f"TIMER: Second pass progress - {nodes_connected} nodes with {children_connected} connections")
		else:
			# Direct processing for smaller DOMs
			for id, node_data in js_node_map.items():
				if id not in node_map:
					continue
				
				# Skip if not an element node or has no children
				if not isinstance(node_map[id], DOMElementNode) or 'children' not in node_data:
					continue
				
				parent_node = node_map[id]
				nodes_connected += 1
				
				# Process children
				for child_id in node_data['children']:
					if child_id not in node_map:
						continue

					child_node = node_map[child_id]
					child_node.parent = parent_node
					parent_node.children.append(child_node)
					children_connected += 1
				
				if nodes_connected % 1000 == 0:
					logger.info(f"TIMER: Second pass progress - {nodes_connected} nodes with {children_connected} connections")
				
		t_second_pass_end = time.time()
		logger.info(f"TIMER: Second pass - {t_second_pass_end-t_second_pass_start:.4f}s for {nodes_connected} nodes and {children_connected} connections")

		html_to_dict = node_map.get(str(js_root_id))
		
		if html_to_dict is None or not isinstance(html_to_dict, DOMElementNode):
			raise ValueError('Failed to parse HTML to dictionary')

		t_cleanup_start = time.time()
		# Release references to allow garbage collection
		js_node_map.clear()
		node_map.clear()
		if 'parent_child_map' in locals():
			parent_child_map.clear()

		gc.collect()
		t_cleanup_end = time.time()
		logger.info(f"TIMER: Cleanup - {t_cleanup_end-t_cleanup_start:.4f}s")

		# Print Python performance summary at the end of each run
		print_timing_summary()
		
		return html_to_dict, selector_map

	@timer("parse_node")
	def _parse_node(
		self,
		node_data: dict,
	) -> Optional[DOMBaseNode]:
		if not node_data:
			return None

		# Process text nodes immediately
		if node_data.get('type') == 'TEXT_NODE':
			# Use __slots__ for text nodes to reduce memory usage
			text_node = DOMTextNode(
				text=node_data['text'],
				is_visible=node_data['isVisible'],
				parent=None,
			)
			return text_node

		# Skip unnecessary operations for invisibles nodes that aren't highlighted
		if (not node_data.get('isVisible', False) and 
			not node_data.get('isInteractive', False) and 
			node_data.get('highlightIndex') is None):
			# For non-visible, non-interactive, non-highlighted nodes, create minimal representation
			element_node = DOMElementNode(
				tag_name=node_data['tagName'],
				xpath=node_data['xpath'],
				attributes={},  # Skip attributes to save memory
				children=[],
				is_visible=False,
				is_interactive=False,
				is_top_element=False,
				is_in_viewport=False,
				highlight_index=None,
				shadow_root=False,
				parent=None,
				viewport_info=None,
			)
			return element_node

		# For important nodes, process everything
		# Process coordinates if they exist for element nodes
		viewport_info = None
		if 'viewport' in node_data:
			viewport_info = ViewportInfo(
				width=node_data['viewport']['width'],
				height=node_data['viewport']['height'],
			)

		# Filter attributes to only keep the most useful ones
		attributes = node_data.get('attributes', {})
		if len(attributes) > 20:  # If there are too many attributes, keep only the important ones
			filtered_attributes = {}
			# Keep only attributes that are likely useful for interaction
			important_attrs = {'id', 'class', 'name', 'type', 'value', 'href', 'src', 'alt', 'title', 'placeholder', 'aria-label'}
			for key, value in attributes.items():
				if key in important_attrs:
					filtered_attributes[key] = value
			attributes = filtered_attributes

		element_node = DOMElementNode(
			tag_name=node_data['tagName'],
			xpath=node_data['xpath'],
			attributes=attributes,
			children=[],
			is_visible=node_data.get('isVisible', False),
			is_interactive=node_data.get('isInteractive', False),
			is_top_element=node_data.get('isTopElement', False),
			is_in_viewport=node_data.get('isInViewport', False),
			highlight_index=node_data.get('highlightIndex'),
			shadow_root=node_data.get('shadowRoot', False),
			parent=None,
			viewport_info=viewport_info,
		)

		return element_node
		
	@time_execution_async('--get_performance_metrics')
	async def get_performance_metrics(self, debug_mode=True):
		"""
		Run a performance analysis and return detailed metrics.
		
		Args:
			debug_mode: Enable detailed JavaScript performance tracking
			
		Returns:
			dict: Performance metrics from both Python and JavaScript
		"""
		args = {
			'doHighlightElements': False,
			'focusHighlightIndex': -1,
			'viewportExpansion': -1,  # Full page
			'debugMode': debug_mode,
		}
		
		logger.info("Running performance analysis...")
		t0 = time.time()
		
		# Reset timer data
		for category in TIMERS:
			TIMERS[category] = {}
		
		result = await self.page.evaluate(self.js_code, args)
		
		# Process result
		try:
			if isinstance(result, str):
				perf_data = json.loads(result)
			else:
				perf_data = result
		except json.JSONDecodeError:
			logger.error("Failed to parse performance data")
			return None
			
		t_end = time.time()
		
		# Process JavaScript timing data
		js_perf = {}
		if isinstance(perf_data, dict) and 'perfSummary' in perf_data:
			js_perf = perf_data['perfSummary']
			
			# Log JavaScript performance summary
			logger.info(f"===== JS PERFORMANCE SUMMARY =====")
			logger.info(f"Total JS processing time: {js_perf['totalTimeMs']}ms for {js_perf['nodeCount']} nodes")
			
			# Log sections by percentage
			sections = sorted(
				[(k, v) for k, v in js_perf['sections'].items()], 
				key=lambda x: float(x[1]['percentage']), 
				reverse=True
			)
			
			for section, data in sections:
				logger.info(f"  {section}: {data['timeMs']}ms ({data['percentage']}%) - {data['calls']} calls")
				
			# Log operations by percentage
			if 'operations' in js_perf:
				operations = sorted(
					[(k, v) for k, v in js_perf['operations'].items()], 
					key=lambda x: float(x[1]['percentage']), 
					reverse=True
				)
				
				for op, data in operations:
					logger.info(f"  {op}: {data['timeMs']}ms ({data['percentage']}%) - {data['calls']} calls, avg {data['avgTimeMs']}ms per call")
			
			logger.info("==================================")
		
		# Create complete performance report
		performance_report = {
			"total_time": t_end - t0,
			"python_timers": {
				"total_time": dict(TIMERS["total_time"]),
				"call_count": dict(TIMERS["call_count"]),
				"max_time": dict(TIMERS["max_time"]),
				"min_time": dict(TIMERS["min_time"]),
			},
			"js_performance": js_perf,
			"js_metrics": perf_data.get('perfMetrics')
		}
		
		return performance_report
