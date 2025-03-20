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
    
    for name, total in sorted_timers:
        calls = TIMERS["call_count"][name]
        avg = total / calls
        max_time = TIMERS["max_time"][name]
        min_time = TIMERS["min_time"][name]
        
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
		element_tree, selector_map = await self._build_dom_tree(highlight_elements, focus_element, viewport_expansion)
		return DOMState(element_tree=element_tree, selector_map=selector_map)

	@time_execution_async('--build_dom_tree')
	@timer("build_dom_tree")
	async def _build_dom_tree(
		self,
		highlight_elements: bool,
		focus_element: int,
		viewport_expansion: int,
	) -> tuple[DOMElementNode, SelectorMap]:
		args = {
			'doHighlightElements': highlight_elements,
			'focusHighlightIndex': focus_element,
			'viewportExpansion': viewport_expansion,
			'debugMode': True,  # Always enable debug mode for performance tracking
		}
		
		t0 = time.time()
		result = await self.page.evaluate(self.js_code, args)
		t_evaluate = time.time()
		logger.info(f"TIMER: page.evaluate - {t_evaluate-t0:.4f}s")
		
		# Handle the JSON string returned from JavaScript
		try:
			if isinstance(result, str):
				t_before_parse = time.time()
				eval_page = json.loads(result)
				t_after_parse = time.time()
				logger.info(f"TIMER: json.loads - {t_after_parse-t_before_parse:.4f}s (size: {len(result)} bytes)")
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
		
		logger.info(f"TIMER: DOM size - {len(js_node_map)} nodes")

		selector_map = {}
		node_map = {}

		# First pass: create all nodes
		t_first_pass_start = time.time()
		nodes_processed = 0
		for id, node_data in js_node_map.items():
			node = self._parse_node(node_data)
			nodes_processed += 1
			
			if nodes_processed % 1000 == 0:
				logger.info(f"TIMER: First pass progress - {nodes_processed}/{len(js_node_map)} nodes processed")
			
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

		html_to_dict = node_map[str(js_root_id)]

		t_cleanup_start = time.time()
		del node_map
		del js_node_map
		del js_root_id

		gc.collect()
		t_cleanup_end = time.time()
		logger.info(f"TIMER: Cleanup - {t_cleanup_end-t_cleanup_start:.4f}s")

		if html_to_dict is None or not isinstance(html_to_dict, DOMElementNode):
			raise ValueError('Failed to parse HTML to dictionary')

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
			text_node = DOMTextNode(
				text=node_data['text'],
				is_visible=node_data['isVisible'],
				parent=None,
			)
			return text_node

		# Process coordinates if they exist for element nodes
		t_viewport_start = time.time()
		viewport_info = None

		if 'viewport' in node_data:
			viewport_info = ViewportInfo(
				width=node_data['viewport']['width'],
				height=node_data['viewport']['height'],
			)
		t_viewport_end = time.time()
		
		if t_viewport_end - t_viewport_start > 0.001:  # Only log if significant
			logger.debug(f"TIMER: viewport processing - {t_viewport_end-t_viewport_start:.4f}s")

		t_element_start = time.time()
		element_node = DOMElementNode(
			tag_name=node_data['tagName'],
			xpath=node_data['xpath'],
			attributes=node_data.get('attributes', {}),
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
		t_element_end = time.time()
		
		if t_element_end - t_element_start > 0.001:  # Only log if significant
			logger.debug(f"TIMER: element node creation - {t_element_end-t_element_start:.4f}s")

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
